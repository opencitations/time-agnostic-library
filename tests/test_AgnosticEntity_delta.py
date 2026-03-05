#!/usr/bin/python
# Copyright (c) 2025, Arcangelo Massari <arcangelo.massari@unibo.it>
#
# Permission to use, copy, modify, and/or distribute this software for any purpose
# with or without fee is hereby granted, provided that the above copyright notice
# and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
# REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
# FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT,
# OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE,
# DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS
# ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
# SOFTWARE.

from time_agnostic_library.agnostic_entity import AgnosticEntity
from triplestore_config import CONFIG

_AR = "https://github.com/arcangelo7/time_agnostic/ar"
_RA = "https://github.com/arcangelo7/time_agnostic/ra"
_AR_GRAPH = f"<{_AR}/>"
_AR_15519 = f"<{_AR}/15519>"
_HELD_BY = "<http://purl.org/spar/pro/isHeldBy>"
_RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
_ROLE_IN_TIME = "<http://purl.org/spar/pro/RoleInTime>"
_WITH_ROLE = "<http://purl.org/spar/pro/withRole>"
_AUTHOR = "<http://purl.org/spar/pro/author>"
_HAS_NEXT = "<https://w3id.org/oc/ontology/hasNext>"


class TestAgnosticEntityDelta:
    def test_get_delta_between_two_versions(self):
        entity = AgnosticEntity(f"{_AR}/15519", config=CONFIG)
        additions, deletions = entity.get_delta(
            "2021-05-07T09:59:15+00:00",
            "2021-06-01T18:46:41+00:00",
        )
        assert additions == {
            (_AR_15519, _HELD_BY, f"<{_RA}/4>", _AR_GRAPH),
            (_AR_15519, _RDF_TYPE, _ROLE_IN_TIME, _AR_GRAPH),
        }
        assert deletions == {
            (_AR_15519, _HELD_BY, f"<{_RA}/15519>", _AR_GRAPH),
        }

    def test_get_delta_same_version(self):
        entity = AgnosticEntity(f"{_AR}/15519", config=CONFIG)
        additions, deletions = entity.get_delta(
            "2021-06-01T18:46:41+00:00",
            "2021-06-01T18:46:41+00:00",
        )
        assert additions == set()
        assert deletions == set()

    def test_get_delta_nonexistent_entity(self):
        entity = AgnosticEntity("https://example.com/nonexistent", config=CONFIG)
        additions, deletions = entity.get_delta(
            "2021-05-07T09:59:15+00:00",
            "2021-06-01T18:46:41+00:00",
        )
        assert additions == set()
        assert deletions == set()

    def test_get_delta_entity_created_within_range(self):
        entity = AgnosticEntity(f"{_AR}/15519", config=CONFIG)
        additions, deletions = entity.get_delta(
            "2020-01-01T00:00:00+00:00",
            "2021-06-01T18:46:41+00:00",
        )
        assert additions == {
            (_AR_15519, _HELD_BY, f"<{_RA}/4>", _AR_GRAPH),
            (_AR_15519, _WITH_ROLE, _AUTHOR, _AR_GRAPH),
            (_AR_15519, _RDF_TYPE, _ROLE_IN_TIME, _AR_GRAPH),
            (_AR_15519, _HAS_NEXT, f"<{_AR}/15520>", _AR_GRAPH),
        }
        assert deletions == set()
