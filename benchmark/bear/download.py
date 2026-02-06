import tarfile
import zipfile
from pathlib import Path
from urllib.request import urlretrieve

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TransferSpeedColumn,
)

console = Console()

BASE_URL = "https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B"
DATA_DIR = Path(__file__).parent / "data" / "daily"

DATASETS = {
    "ic": f"{BASE_URL}/datasets/day/IC/alldata.IC.nt.tar.gz",
    "cb": f"{BASE_URL}/datasets/day/CB/alldata.CB.nt.tar.gz",
}

QUERIES = {
    "p": f"{BASE_URL}/Queries/p/p.txt",
    "po": f"{BASE_URL}/Queries/po/po.txt",
}

RESULTS = {
    "p": {
        "mat": f"{BASE_URL}/results/day/p/mat-p-queries.zip",
        "ver": f"{BASE_URL}/results/day/p/ver-p-queries.zip",
        "diff": f"{BASE_URL}/results/day/p/diff-p-queries.zip",
    },
    "po": {
        "mat": f"{BASE_URL}/results/day/po/mat-po-queries.zip",
        "ver": f"{BASE_URL}/results/day/po/ver-po-queries.zip",
        "diff": f"{BASE_URL}/results/day/po/diff-po-queries.zip",
    },
}


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
    console.rule("[bold]BEAR-B-daily dataset")
    ic_archive = DATA_DIR / "alldata.IC.nt.tar.gz"
    download_file(DATASETS["ic"], ic_archive)
    extract_tar_gz(ic_archive, DATA_DIR / "IC")

    cb_archive = DATA_DIR / "alldata.CB.nt.tar.gz"
    download_file(DATASETS["cb"], cb_archive)
    extract_tar_gz(cb_archive, DATA_DIR / "CB")

    console.rule("[bold]Query files")
    queries_dir = DATA_DIR.parent / "queries"
    queries_dir.mkdir(parents=True, exist_ok=True)
    for pattern_type, url in QUERIES.items():
        download_file(url, queries_dir / f"{pattern_type}.txt")

    console.rule("[bold]BEAR-B-daily results")
    results_dir = DATA_DIR / "results"
    for pattern_type, urls in RESULTS.items():
        for query_type, url in urls.items():
            archive = results_dir / pattern_type / f"{query_type}.zip"
            download_file(url, archive)
            extract_zip(archive, results_dir / pattern_type / query_type)

    console.print("\n[bold green]Done.")


if __name__ == "__main__":
    main()
