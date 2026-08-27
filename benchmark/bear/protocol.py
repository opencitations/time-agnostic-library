# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import hashlib
import json
import os
import platform
import subprocess
from pathlib import Path

from corpora import Corpus
from parse_queries import load_query_set

DEFAULT_REPLICATIONS = 3
DEFAULT_TIMEOUT_S = 600

REPOSITORY_ROOT = Path(__file__).parents[2]


def hardware_info() -> dict[str, str | int]:
    info: dict[str, str | int] = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "python_version": platform.python_version(),
    }
    try:
        result = subprocess.run(["nproc"], capture_output=True, text=True, check=True)
        info["cpu_cores"] = int(result.stdout.strip())
    except (OSError, subprocess.CalledProcessError, ValueError):
        info["cpu_cores"] = os.cpu_count() or 1
    try:
        with Path("/proc/meminfo").open() as file:
            for line in file:
                if line.startswith("MemTotal:"):
                    info["memory_total_kb"] = int(line.split()[1])
                    break
    except (OSError, ValueError):
        pass
    return info


def build_manifest(corpus: Corpus) -> dict:
    query_sets = []
    for query_set in corpus.queries:
        patterns = [
            {"pattern_index": index, "triple": list(triple)}
            for index, triple in enumerate(load_query_set(query_set))
        ]
        query_sets.append({"name": query_set.name, "patterns": patterns})

    return {
        "corpus": corpus.name,
        "query_sets": query_sets,
    }


def manifest_hash(manifest: dict) -> str:
    serialized = json.dumps(manifest, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode()).hexdigest()


def manifest_patterns(manifest: dict, query_set_name: str) -> list[dict]:
    for query_set in manifest["query_sets"]:
        if query_set["name"] == query_set_name:
            return query_set["patterns"]
    msg = f"Query set not found in manifest: {query_set_name}"
    raise ValueError(msg)


def git_revision(repository: Path = REPOSITORY_ROOT) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repository,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def git_dirty(repository: Path = REPOSITORY_ROOT) -> bool | None:
    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repository,
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return bool(result.stdout)


def docker_image_id(image_name: str) -> str | None:
    try:
        result = subprocess.run(
            ["docker", "image", "inspect", image_name, "--format", "{{.Id}}"],
            capture_output=True,
            text=True,
            check=True,
        )
    except (OSError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def protocol_metadata(
    manifest: dict,
    replications: int,
    *,
    measurement: str,
    timeout_s: int | None,
) -> dict:
    return {
        "measurement": measurement,
        "replications": replications,
        "per_case_statistic": "median",
        "warmup": "one untimed execution per case",
        "timeout_s": timeout_s,
        "git_revision": git_revision(),
        "git_dirty": git_dirty(),
        "manifest_hash": manifest_hash(manifest),
        "manifest": manifest,
    }
