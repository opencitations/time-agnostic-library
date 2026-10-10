# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import os
import subprocess
import sys
import time
from pathlib import Path

import corpora
import requests
from protocol import docker_image_id

SCRIPT_DIR = Path(__file__).resolve().parent
OSTRICH_REVISION = "a1f362bd1e97751206dd102b40750a12b950f797"
SYSTEM_IMAGES = {
    "ostrich": f"ostrich-bear:{OSTRICH_REVISION}",
    "tal-qlever": (
        "docker.io/adfreiburg/qlever:0.6.0@sha256:"
        "37d5ede193f1bffb6aebf734d15d2a4c2a3228ee102858b0c6c2e65c149a78ec"
    ),
    "r43ples": (
        "docker.io/plttud/r43ples@sha256:"
        "70a2c27c93c2e0a3eba3920bcbf9a3887dc023bf0d222d4237e5e6c5c956996c"
    ),
}


def ostrich_repository() -> Path:
    return corpora.DATA_DIR / "ostrich" / f"ostrich-repo-{OSTRICH_REVISION}"


def prepare_image(system: str) -> str:
    image = SYSTEM_IMAGES[system]
    image_id = docker_image_id(image)
    if image_id is not None:
        return image_id
    if system == "ostrich":
        repository = ostrich_repository()
        if not repository.exists():
            repository.parent.mkdir(parents=True, exist_ok=True)
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--no-checkout",
                    "https://github.com/rdfostrich/ostrich.git",
                    str(repository),
                ],
                check=True,
            )
        subprocess.run(
            ["git", "-C", str(repository), "checkout", "--detach", OSTRICH_REVISION],
            check=True,
        )
        subprocess.run(
            [
                "git",
                "-C",
                str(repository),
                "submodule",
                "update",
                "--init",
                "--recursive",
            ],
            check=True,
        )
        dockerfile = (
            (repository / "Dockerfile")
            .read_text(encoding="utf-8")
            .replace(
                "RUN cd /opt && curl -LO https://boostorg.jfrog.io/artifactory/main/"
                "release/1.80.0/source/boost_1_80_0.tar.gz",
                "RUN cd /opt && curl -fLO https://archives.boost.io/"
                "release/1.80.0/source/boost_1_80_0.tar.gz"
                " && echo '"
                "4b2136f98bdd1f5857f1c3dea9ac2018effe65286cf251534b6ae20cc45e1847"
                "  boost_1_80_0.tar.gz' | sha256sum -c -",
            )
        )
        subprocess.run(
            ["docker", "build", "--file", "-", "-t", image, str(repository)],
            input=dockerfile,
            text=True,
            check=True,
        )
    else:
        subprocess.run(["docker", "pull", image], check=True)
    return subprocess.check_output(
        ["docker", "image", "inspect", image, "--format", "{{.Id}}"], text=True
    ).strip()


def build_ostrich(corpus: corpora.Corpus) -> None:
    directory = corpora.DATA_DIR / "ostrich"
    patches = directory / f"patches_{corpus.name}"
    store = directory / f"evalrun_{corpus.name}"
    for kind in ("IC", "CB"):
        destination = patches / f"alldata.{kind}.nt"
        destination.mkdir(parents=True)
        with os.scandir(corpus.dir / kind) as entries:
            for entry in entries:
                if entry.name.endswith(".nt") and entry.is_file():
                    os.link(entry.path, destination / entry.name)
    strategies = {
        "bear-a": ("interval", "2"),
        "bear-b-daily": ("interval", "5"),
        "bear-b-hourly": ("interval", "50"),
        "bear-b-instant": ("time", "20"),
    }
    store.mkdir()
    command = [
        "docker",
        "run",
        "--rm",
        "--ulimit",
        "nofile=65536:65536",
        "-v",
        f"{store}:/var/evalrun",
        "-v",
        f"{patches}:/var/patches",
        SYSTEM_IMAGES["ostrich"],
        "ingest",
        *strategies[corpus.name],
        "/var/patches",
        "1",
        str(corpus.num_versions),
    ]
    log = directory / f"ingestion_output_{corpus.name}.txt"
    print(f"OSTRICH ingestion log: {log}", flush=True)
    with log.open("w", encoding="utf-8") as output:
        subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=True)


def wait_for_r43ples() -> None:
    endpoint = "http://localhost:9998/r43ples/sparql"
    for _ in range(30):
        try:
            with requests.get(endpoint, timeout=1):
                return
        except (requests.ConnectionError, requests.Timeout):  # noqa: PERF203
            time.sleep(1)
    msg = "R43ples did not become ready within 30 attempts."
    raise TimeoutError(msg)


def start_r43ples(corpus: corpora.Corpus) -> None:
    volume = f"r43ples-data-{corpus.name}"
    subprocess.run(
        [
            "docker",
            "run",
            "-d",
            "--name",
            f"r43ples-bear-{corpus.name}",
            "-p",
            "9998:9998",
            "-v",
            f"{volume}:/r43ples/database",
            SYSTEM_IMAGES["r43ples"],
            "java",
            "-Xmx8g",
            "-jar",
            "r43ples.jar",
        ],
        check=True,
    )
    wait_for_r43ples()


def build_r43ples(corpus: corpora.Corpus) -> float:
    subprocess.run(
        ["docker", "volume", "create", f"r43ples-data-{corpus.name}"], check=True
    )
    start_r43ples(corpus)
    start = time.perf_counter()
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "ingest_r43ples.py"),
            "--corpus",
            corpus.name,
            "--port",
            "9998",
        ],
        check=True,
    )
    return time.perf_counter() - start
