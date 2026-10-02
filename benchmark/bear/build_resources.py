# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import json
import os
import stat
import subprocess
from pathlib import Path

CGROUP_ROOT = Path("/sys/fs/cgroup")


def start_slice(name: str) -> Path:
    subprocess.run(
        [
            "sudo",
            "-n",
            "busctl",
            "call",
            "org.freedesktop.systemd1",
            "/org/freedesktop/systemd1",
            "org.freedesktop.systemd1.Manager",
            "StartTransientUnit",
            "ssa(sv)a(sa(sv))",
            name,
            "fail",
            "1",
            "MemoryAccounting",
            "b",
            "true",
            "0",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    control_group = subprocess.check_output(
        ["systemctl", "show", name, "--property=ControlGroup", "--value"],
        text=True,
    ).strip()
    return CGROUP_ROOT / control_group.lstrip("/")


def stop_slice(name: str) -> None:
    subprocess.run(["sudo", "-n", "systemctl", "stop", name], check=True)


def memory_usage(cgroup: Path) -> dict[str, int]:
    events = dict(
        line.split() for line in (cgroup / "memory.events").read_text().splitlines()
    )
    return {
        "memory_peak_bytes": int((cgroup / "memory.peak").read_text()),
        "oom_kills": int(events["oom_kill"]),
    }


def storage_usage(directory: Path, system: str, corpus: str) -> dict[str, int]:
    def raise_walk_error(error: OSError) -> None:
        raise error

    apparent = allocated = 0
    seen: set[tuple[int, int]] = set()
    for root, _, files in os.walk(directory, onerror=raise_walk_error):
        for name in files:
            if system == "tal-qlever" and not name.startswith(
                (
                    f"{corpus}.index.",
                    f"{corpus}.internal.index.",
                    f"{corpus}.vocabulary.",
                    f"{corpus}.meta-data.json",
                )
            ):
                continue
            file_stat = (Path(root) / name).stat()
            identity = (file_stat.st_dev, file_stat.st_ino)
            if stat.S_ISREG(file_stat.st_mode) and identity not in seen:
                seen.add(identity)
                apparent += file_stat.st_size
                allocated += file_stat.st_blocks * 512
    return {"store_bytes": apparent, "store_allocated_bytes": allocated}


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument("directory", type=Path)
    parser.add_argument("system")
    parser.add_argument("corpus")
    args = parser.parse_args()
    if not args.directory.is_dir():
        parser.error(f"Store directory does not exist: {args.directory}")
    print(json.dumps(storage_usage(args.directory, args.system, args.corpus)))


if __name__ == "__main__":  # pragma: no cover
    main()
