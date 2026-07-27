# SPDX-FileCopyrightText: 2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

"""The BEAR corpora and query sets the benchmark runs on."""

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

BEAR_URL = "https://aic.ai.wu.ac.at/qadlod/bear"

# BEAR does not date its versions, so the benchmark lays them out on a synthetic
# timeline whose step is the granularity of the corpus.
BASE_TIMESTAMP = datetime(2015, 8, 1, tzinfo=timezone.utc)

DATA_DIR = Path(__file__).parent / "data"
QUERIES_DIR = DATA_DIR / "queries"

QUERY_ATOMS = ("mat", "ver", "diff")


@dataclass(frozen=True)
class QuerySet:
    """A file of triple patterns distributed with BEAR, one per line."""

    name: str
    url: str
    path: Path
    results: dict[str, str] = field(default_factory=dict)

    def results_stem(self, atom: str) -> str:
        """Name BEAR gives to the archive of expected results, and to its files."""
        return self.results[atom].rsplit("/", 1)[-1].removesuffix(".zip")


@dataclass(frozen=True)
class Corpus:
    """A BEAR data corpus and everything needed to benchmark on it."""

    name: str
    ic_url: str
    # Only OSTRICH reads the changesets: the OCDM conversion uses the IC policy,
    # because the published CB deltas disagree with the IC snapshots.
    cb_url: str
    num_versions: int
    interval: timedelta
    port: int
    dm_step: int
    queries: tuple[QuerySet, ...]

    @property
    def dir(self) -> Path:
        return DATA_DIR / self.name

    def endpoint(self) -> str:
        return f"http://localhost:{self.port}/sparql"

    def expected_results(self, query_set: QuerySet, atom: str, index: int) -> Path:
        stem = query_set.results_stem(atom)
        directory = self.dir / "results" / query_set.name / atom / stem
        return directory / f"{stem}-{index}.txt"

    def sample_versions(self) -> list[int]:
        last = self.num_versions - 1
        return sorted({0, last // 2, last})

    def datetimes(self, count: int | None = None) -> list[datetime]:
        versions = self.num_versions if count is None else count
        return [BASE_TIMESTAMP + self.interval * i for i in range(versions)]

    def timestamps(self, count: int | None = None) -> list[str]:
        return [
            moment.strftime("%Y-%m-%dT%H:%M:%S+00:00")
            for moment in self.datetimes(count)
        ]


def _bear_b_queries(granularity: str) -> tuple[QuerySet, ...]:
    base = f"{BEAR_URL}/BEAR_B"
    # BEAR publishes no expected results for the instant granularity.
    published = granularity != "instant"
    return tuple(
        QuerySet(
            name=name,
            url=f"{base}/Queries/{name}/{name}.txt",
            path=QUERIES_DIR / "bear-b" / f"{name}.txt",
            results={
                atom: f"{base}/results/{granularity}/{name}/{atom}-{name}-queries.zip"
                for atom in QUERY_ATOMS
            }
            if published
            else {},
        )
        for name in ("p", "po")
    )


def _bear_a_query_set(
    name: str, kind: str, query_stem: str, results_stem: str
) -> QuerySet:
    base = f"{BEAR_URL}/BEAR_A"
    return QuerySet(
        name=name,
        url=f"{base}/Queries/{kind}/{query_stem}.txt",
        path=QUERIES_DIR / "bear-a" / f"{query_stem}.txt",
        results={
            atom: f"{base}/results/{kind}/{atom}-{results_stem}.zip"
            for atom in QUERY_ATOMS
        },
    )


def _bear_a_queries() -> tuple[QuerySet, ...]:
    sets = []
    for kind in ("s", "p", "o", "sp", "po"):
        for cardinality in ("low", "high"):
            stem = f"{kind}-queries-{cardinality}Cardinality"
            sets.append(_bear_a_query_set(f"{kind}-{cardinality}", kind, stem, stem))
    # so is published for low cardinality only, and spo carries no cardinality
    # in the name of its query file while its results keep one.
    so_stem = "so-queries-lowCardinality"
    sets.append(_bear_a_query_set("so-low", "so", so_stem, so_stem))
    sets.append(
        _bear_a_query_set("spo", "spo", "spo-queries", "spo-queries-lowCardinality")
    )
    return tuple(sets)


CORPORA = {
    corpus.name: corpus
    for corpus in (
        Corpus(
            name="bear-b-daily",
            ic_url=f"{BEAR_URL}/BEAR_B/datasets/day/IC/alldata.IC.nt.tar.gz",
            cb_url=f"{BEAR_URL}/BEAR_B/datasets/day/CB/alldata.CB.nt.tar.gz",
            num_versions=89,
            interval=timedelta(days=1),
            port=7001,
            dm_step=5,
            queries=_bear_b_queries("day"),
        ),
        Corpus(
            name="bear-b-hourly",
            ic_url=f"{BEAR_URL}/BEAR_B/datasets/hour/IC/alldata.IC.nt.tar.gz",
            cb_url=f"{BEAR_URL}/BEAR_B/datasets/hour/CB/alldata.CB.nt.tar.gz",
            num_versions=1299,
            interval=timedelta(hours=1),
            port=7002,
            dm_step=100,
            queries=_bear_b_queries("hour"),
        ),
        Corpus(
            name="bear-b-instant",
            ic_url=f"{BEAR_URL}/BEAR_B/datasets/instant/IC/alldata.IC.nt.tar.gz",
            cb_url=f"{BEAR_URL}/BEAR_B/datasets/instant/CB/alldata.CB.nt.tar.gz",
            num_versions=21046,
            interval=timedelta(minutes=1),
            port=7003,
            dm_step=1500,
            queries=_bear_b_queries("instant"),
        ),
        Corpus(
            name="bear-a",
            ic_url=f"{BEAR_URL}/BEAR_A/datasets/IC/alldata.IC.nt.tar.gz",
            cb_url=f"{BEAR_URL}/BEAR_A/datasets/CB/alldata.CB.nt.tar.gz",
            num_versions=58,
            interval=timedelta(weeks=1),
            port=7004,
            dm_step=5,
            queries=_bear_a_queries(),
        ),
    )
}

CORPUS_NAMES = tuple(CORPORA)


def get(name: str) -> Corpus:
    return CORPORA[name]


# Every corpus is served by Virtuoso with its free-text index enabled.
def build_config(corpus: Corpus) -> dict:
    source = {
        "triplestore_urls": [corpus.endpoint()],
        "file_paths": [],
        "is_quadstore": True,
    }
    return {
        "dataset": source,
        "provenance": source,
        "blazegraph_full_text_search": "no",
        "fuseki_full_text_search": "no",
        "virtuoso_full_text_search": "yes",
        "graphdb_connector_name": "",
    }
