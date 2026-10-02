# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import corpora
from build_resources import memory_usage, start_slice, stop_slice
from protocol import docker_image_id, git_dirty, git_revision, hardware_info

SCRIPT_DIR = Path(__file__).resolve().parent
SYSTEM_IMAGES = {
    "ostrich": "ostrich-bear",
    "tal-qlever": "docker.io/adfreiburg/qlever",
    "r43ples": "plttud/r43ples:latest",
}


def build_plan(system: str, corpus: corpora.Corpus) -> list[tuple[str, list[str]]]:
    stages = []
    if system == "tal-qlever":
        stages.append(
            (
                "conversion",
                [
                    sys.executable,
                    str(SCRIPT_DIR / "convert_to_ocdm.py"),
                    "--corpus",
                    corpus.name,
                ],
            )
        )
        command = [sys.executable, str(SCRIPT_DIR / "setup_qlever.py"), corpus.name]
    else:
        command = ["bash", str(SCRIPT_DIR / f"setup_{system}.sh"), corpus.name]
    stages.append(("store", command))
    return stages


def store_directory(system: str, corpus: corpora.Corpus) -> Path:
    if system == "ostrich":
        return corpora.DATA_DIR / "ostrich" / f"evalrun_{corpus.name}"
    if system == "r43ples":
        mountpoint = subprocess.check_output(
            [
                "docker",
                "volume",
                "inspect",
                f"r43ples-data-{corpus.name}",
                "--format",
                "{{.Mountpoint}}",
            ],
            text=True,
        ).strip()
        return Path(mountpoint)
    return corpus.dir / "qlever-data"


def check_fresh_build(system: str, corpus: corpora.Corpus, output: Path) -> None:
    paths = [output]
    if system == "r43ples":
        volumes = subprocess.check_output(
            ["docker", "volume", "ls", "--format", "{{.Name}}"],
            text=True,
        ).splitlines()
        if f"r43ples-data-{corpus.name}" in volumes:
            msg = "Preserve or remove the existing R43ples volume before a fresh build."
            raise FileExistsError(msg)
        paths.append(corpora.DATA_DIR / f"r43ples_ingestion_time_{corpus.name}.json")
        containers = [f"r43ples-bear-{corpus.name}"]
    else:
        paths.append(store_directory(system, corpus))
        if system == "ostrich":
            paths.append(corpora.DATA_DIR / "ostrich" / f"patches_{corpus.name}")
            containers = []
        else:
            containers = [f"qlever-{corpus.name}", f"qlever-{corpus.name}-index"]
    existing_containers = subprocess.check_output(
        ["docker", "ps", "-a", "--format", "{{.Names}}"],
        text=True,
    ).splitlines()
    if set(containers).intersection(existing_containers):
        msg = "Preserve or remove existing target containers before a fresh build."
        raise FileExistsError(msg)
    if system == "tal-qlever":
        paths.extend(
            corpus.dir / name for name in ("dataset.nq.gz", "provenance.nq.gz")
        )
    for path in paths:
        if path.exists():
            msg = f"Fresh build requires an unused output path: {path}"
            raise FileExistsError(msg)
    for kind in ("IC", "CB") if system in {"ostrich", "r43ples"} else ("IC",):
        if not (corpus.dir / kind).is_dir():
            msg = f"Download the {corpus.name} {kind} inputs before measuring a build."
            raise FileNotFoundError(msg)


def run_stage(command: list[str], slice_name: str, run_id: str, docker: str) -> int:
    environment = {
        "PATH": os.pathsep.join(
            [
                str(SCRIPT_DIR / "build_tools"),
                str(Path(sys.executable).parent),
                os.environ["PATH"],
            ]
        ),
        "BEAR_DOCKER": docker,
        "BEAR_BUILD_SLICE": slice_name,
        "BEAR_BUILD_ID": run_id,
        "UV_NO_SYNC": "1",
    }
    result = subprocess.run(
        [
            "sudo",
            "-n",
            "systemd-run",
            "--quiet",
            "--wait",
            "--pipe",
            "--collect",
            f"--unit={slice_name.removesuffix('.slice')}.service",
            f"--slice={slice_name}",
            f"--uid={os.getuid()}",
            f"--gid={os.getgid()}",
            f"--working-directory={SCRIPT_DIR.parents[1]}",
            "--property=MemoryAccounting=yes",
            *[f"--setenv={key}={value}" for key, value in environment.items()],
            "--",
            *command,
        ],
        check=False,
    )
    return result.returncode


def stop_build_containers(run_id: str) -> None:
    containers = subprocess.check_output(
        ["docker", "ps", "-q", "--filter", f"label=bear.build={run_id}"],
        text=True,
    ).splitlines()
    if containers:
        subprocess.run(["docker", "stop", *containers], check=True)


def save_record(path: Path, record: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def measure_build(system: str, corpus: corpora.Corpus, output: Path) -> dict:
    check_fresh_build(system, corpus, output)
    docker = shutil.which("docker")
    if docker is None:
        msg = "Docker is required."
        raise FileNotFoundError(msg)
    driver = subprocess.check_output(
        [docker, "info", "--format", "{{.CgroupVersion}} {{.CgroupDriver}}"],
        text=True,
    ).strip()
    if driver != "2 systemd":
        msg = "Build measurement requires Docker with cgroup v2 and the systemd driver."
        raise RuntimeError(msg)
    image = SYSTEM_IMAGES[system]
    image_id = docker_image_id(image)
    if image_id is None:
        msg = f"Prepare the Docker image before measuring: {image}"
        raise RuntimeError(msg)
    run_id = uuid4().hex
    parent = f"bearbuild{run_id}.slice"
    record = {
        "system": system,
        "corpus": corpus.name,
        "status": "running",
        "started_at": datetime.now(timezone.utc).isoformat(),
        "hardware": hardware_info(),
        "protocol": {
            "schema": "store-build-v1",
            "memory_metric": "cgroup-v2-memory.peak",
            "memory_scope": (
                "build processes and containers, including file cache and kernel memory"
            ),
            "time_scope": "downloaded BEAR inputs to query-ready store",
            "cache_policy": "existing host cache; no cache eviction",
            "storage_scope": (
                "query store files; excludes input, intermediate and diagnostic files"
            ),
            "git_revision": git_revision(),
            "git_dirty": git_dirty(),
            "image": image,
            "image_id": image_id,
        },
        "stages": [],
        "elapsed_s": None,
        "memory_peak_bytes": None,
        "oom_kills": None,
        "store_bytes": None,
        "store_allocated_bytes": None,
    }
    with ExitStack() as stack, ExitStack() as containers:
        cgroup = start_slice(parent)
        stack.callback(stop_slice, parent)
        memory_usage(cgroup)
        containers.callback(stop_build_containers, run_id)
        output.parent.mkdir(parents=True, exist_ok=True)
        save_record(output, record)
        start = time.perf_counter()
        try:
            for name, command in build_plan(system, corpus):
                slice_name = f"bearbuild{run_id}-{name}.slice"
                stage_cgroup = start_slice(slice_name)
                stack.callback(stop_slice, slice_name)
                stage = {"name": name, "status": "running", "exit_code": None}
                record["stages"].append(stage)
                save_record(output, record)
                stage_start = time.perf_counter()
                try:
                    stage["exit_code"] = run_stage(command, slice_name, run_id, docker)
                finally:
                    stage["elapsed_s"] = time.perf_counter() - stage_start
                    stage.update(memory_usage(stage_cgroup))
                    stage["status"] = (
                        "complete"
                        if stage["exit_code"] == 0 and stage["oom_kills"] == 0
                        else "failed"
                    )
                    save_record(output, record)
                if stage["status"] != "complete":
                    break
            else:
                record["elapsed_s"] = time.perf_counter() - start
                record.update(memory_usage(cgroup))
                stop_build_containers(run_id)
                usage = subprocess.check_output(
                    [
                        "sudo",
                        "-n",
                        sys.executable,
                        str(SCRIPT_DIR / "build_resources.py"),
                        str(store_directory(system, corpus)),
                        system,
                        corpus.name,
                    ],
                    text=True,
                )
                record.update(json.loads(usage))
                record["status"] = "complete"
        finally:
            if record["elapsed_s"] is None:
                record["elapsed_s"] = time.perf_counter() - start
                record.update(memory_usage(cgroup))
            if record["status"] == "running" or record["oom_kills"]:
                record["status"] = "failed"
            save_record(output, record)
    return record


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", choices=SYSTEM_IMAGES, required=True)
    parser.add_argument("--corpus", choices=corpora.CORPUS_NAMES, required=True)
    args = parser.parse_args()
    output = corpora.DATA_DIR / f"store_build_{args.system}_{args.corpus}.json"
    result = measure_build(args.system, corpora.get(args.corpus), output)
    print(f"Build {result['status']}; measurements: {output}")
    if result["status"] != "complete":
        raise SystemExit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
