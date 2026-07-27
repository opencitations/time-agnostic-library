# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

import argparse
import gzip
import shutil
import tarfile
import zipfile
from pathlib import Path
from urllib.request import Request, urlopen, urlretrieve

import corpora
from corpora import Corpus
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


def get_remote_size(url: str) -> int:
    req = Request(url, method="HEAD")
    with urlopen(req) as resp:
        return int(resp.headers.get("Content-Length", 0))


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


def decompress_ntriples(directory: Path) -> None:
    """Leave every version as plain N-Triples.

    BEAR ships some corpora gzipped inside the archive and others not. Every
    system under benchmark has to read the same bytes, and OSTRICH reads plain
    N-Triples only, so plain is the format the whole pipeline uses. The
    downloaded archive stays on disk, so this is reversible without downloading
    anything again.
    """
    compressed = sorted(directory.rglob("*.nt.gz"))
    if not compressed:
        return
    with Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]Decompressing {task.fields[name]}..."),
        console=console,
    ) as progress:
        progress.add_task("gunzip", name=directory.name, total=None)
        for archive in compressed:
            plain = archive.with_suffix("")
            if not plain.exists():
                with gzip.open(archive, "rb") as src, plain.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
            archive.unlink()
    console.print(f"  Decompressed {len(compressed)} files in {directory}")


def dataset_urls(corpus: Corpus) -> dict[str, str]:
    urls = {"IC": corpus.ic_url}
    if corpus.cb_url:
        urls["CB"] = corpus.cb_url
    return urls


def all_urls(corpus: Corpus) -> list[str]:
    urls = list(dataset_urls(corpus).values())
    for query_set in corpus.queries:
        urls.append(query_set.url)
        urls.extend(query_set.results.values())
    return urls


def download_datasets(corpus: Corpus) -> None:
    for policy, url in dataset_urls(corpus).items():
        console.rule(f"[bold]{corpus.name} {policy} dataset")
        archive = corpus.dir / f"alldata.{policy}.nt.tar.gz"
        download_file(url, archive)
        extract_tar_gz(archive, corpus.dir / policy)
        decompress_ntriples(corpus.dir / policy)


def download_queries(corpus: Corpus) -> None:
    console.rule("[bold]Query files")
    for query_set in corpus.queries:
        download_file(query_set.url, query_set.path)


def download_expected_results(corpus: Corpus) -> None:
    published = [qs for qs in corpus.queries if qs.results]
    if not published:
        console.print("No expected results are published for this corpus")
        return
    console.rule(f"[bold]{corpus.name} expected results")
    for query_set in published:
        for atom, url in query_set.results.items():
            archive = corpus.dir / "results" / query_set.name / f"{atom}.zip"
            download_file(url, archive)
            extract_zip(archive, corpus.dir / "results" / query_set.name / atom)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--corpus", choices=corpora.CORPUS_NAMES, default="bear-b-daily"
    )
    args = parser.parse_args()

    corpus = corpora.get(args.corpus)
    total_bytes = sum(get_remote_size(url) for url in all_urls(corpus))
    console.print(f"Total download size: {total_bytes / 1024**3:.1f} GB")

    download_datasets(corpus)
    download_queries(corpus)
    download_expected_results(corpus)

    console.print("\n[bold green]Done.")


if __name__ == "__main__":
    main()
