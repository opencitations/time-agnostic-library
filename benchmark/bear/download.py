import argparse
import tarfile
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen, urlretrieve

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

console = Console()

BASE_URL = "https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B"

GRANULARITY_CONFIG = {
    "daily": {"url_path": "day", "has_results": True},
    "hourly": {"url_path": "hour", "has_results": True},
    "instant": {"url_path": "instant", "has_results": False},
}


def build_urls(url_path: str) -> tuple[dict[str, str], dict[str, dict[str, str]]]:
    datasets = {
        "ic": f"{BASE_URL}/datasets/{url_path}/IC/alldata.IC.nt.tar.gz",
        "cb": f"{BASE_URL}/datasets/{url_path}/CB/alldata.CB.nt.tar.gz",
    }
    results = {
        "p": {
            "mat": f"{BASE_URL}/results/{url_path}/p/mat-p-queries.zip",
            "ver": f"{BASE_URL}/results/{url_path}/p/ver-p-queries.zip",
            "diff": f"{BASE_URL}/results/{url_path}/p/diff-p-queries.zip",
        },
        "po": {
            "mat": f"{BASE_URL}/results/{url_path}/po/mat-po-queries.zip",
            "ver": f"{BASE_URL}/results/{url_path}/po/ver-po-queries.zip",
            "diff": f"{BASE_URL}/results/{url_path}/po/diff-po-queries.zip",
        },
    }
    return datasets, results


QUERIES = {
    "p": f"{BASE_URL}/Queries/p/p.txt",
    "po": f"{BASE_URL}/Queries/po/po.txt",
}


def get_remote_size(url: str) -> int:
    req = Request(url, method="HEAD")
    with urlopen(req) as resp:
        return int(resp.headers.get("Content-Length", 0))


def get_total_download_size(urls: list[str]) -> int:
    total = 0
    for url in urls:
        total += get_remote_size(url)
    return total


def download_file(url: str, dest: Path) -> None:
    if dest.exists():
        console.print(f"  Already exists: {dest}", style="dim")
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.fields[filename]}"),
        BarColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task("download", filename=dest.name, total=None)

        def reporthook(block_num, block_size, total_size):
            if total_size > 0:
                progress.update(task, total=total_size)
            progress.update(task, completed=block_num * block_size)

        urlretrieve(url, dest, reporthook=reporthook)
    console.print(f"  Saved to {dest}")


def extract_tar_gz(archive: Path, dest_dir: Path) -> None:
    if dest_dir.exists() and any(dest_dir.iterdir()):
        console.print(f"  Already extracted: {dest_dir}", style="dim")
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]Extracting {task.fields[filename]}..."),
        console=console,
    ) as progress:
        progress.add_task("extract", filename=archive.name, total=None)
        with tarfile.open(archive, "r:gz") as tar:
            tar.extractall(dest_dir, filter="data")
    console.print(f"  Extracted to {dest_dir}")


def extract_zip(archive: Path, dest_dir: Path) -> None:
    if dest_dir.exists() and any(dest_dir.iterdir()):
        console.print(f"  Already extracted: {dest_dir}", style="dim")
        return
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, "r") as zf:
        zf.extractall(dest_dir)
    console.print(f"  Extracted to {dest_dir}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--granularity", choices=["daily", "hourly", "instant"], default="daily")
    args = parser.parse_args()

    granularity = args.granularity
    config = GRANULARITY_CONFIG[granularity]
    url_path = config["url_path"]
    has_results = config["has_results"]
    data_dir = Path(__file__).parent / "data" / granularity
    datasets, results = build_urls(url_path)

    all_urls = list(datasets.values())
    if has_results:
        for urls in results.values():
            all_urls.extend(urls.values())
    all_urls.extend(QUERIES.values())
    total_bytes = get_total_download_size(all_urls)
    console.print(f"Total download size: {total_bytes / 1024 / 1024:.1f} MB")

    console.rule(f"[bold]BEAR-B-{granularity} dataset")
    ic_archive = data_dir / "alldata.IC.nt.tar.gz"
    download_file(datasets["ic"], ic_archive)
    extract_tar_gz(ic_archive, data_dir / "IC")

    cb_archive = data_dir / "alldata.CB.nt.tar.gz"
    download_file(datasets["cb"], cb_archive)
    extract_tar_gz(cb_archive, data_dir / "CB")

    console.rule("[bold]Query files")
    queries_dir = data_dir.parent / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)
    for pattern_type, url in QUERIES.items():
        download_file(url, queries_dir / f"{pattern_type}.txt")

    if has_results:
        console.rule(f"[bold]BEAR-B-{granularity} results")
        results_dir = data_dir / "results"
        for pattern_type, urls in results.items():
            for query_type, url in urls.items():
                archive = results_dir / pattern_type / f"{query_type}.zip"
                download_file(url, archive)
                extract_zip(archive, results_dir / pattern_type / query_type)
    else:
        console.print("No pre-computed result files available for this granularity")

    console.print("\n[bold green]Done.")


if __name__ == "__main__":
    main()
