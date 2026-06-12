# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from triplestore_config import CONFIG

from time_agnostic_library.prov_entity import ProvEntity
from time_agnostic_library.sparql import Sparql
from time_agnostic_library.support import _to_nt_sorted_list


class TestSparql:
    def test_run_select_query(self):
        query = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/ar/15519> AS ?s)
                ?s ?p ?o
            }
        """
        output = Sparql(query, CONFIG).run_select_query()
        # Sort bindings by p and o values for consistent comparison
        output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        expected_output = {
            "head": {"vars": ["p", "o"]},
            "results": {
                "bindings": [
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://purl.org/spar/pro/isHeldBy",
                        },
                        "o": {
                            "type": "uri",
                            "value": "https://github.com/arcangelo7/time_agnostic/ra/4",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://purl.org/spar/pro/withRole",
                        },
                        "o": {
                            "type": "uri",
                            "value": "http://purl.org/spar/pro/author",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        },
                        "o": {
                            "type": "uri",
                            "value": "http://purl.org/spar/pro/RoleInTime",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "https://w3id.org/oc/ontology/hasNext",
                        },
                        "o": {
                            "type": "uri",
                            "value": "https://github.com/arcangelo7/time_agnostic/ar/15"
                            "520",
                        },
                    },
                ]
            },
        }
        # Sort expected bindings in the same way
        expected_output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        assert output == expected_output

    def test__get_results_from_files(self):
        input_1 = f"""
            SELECT ?p ?o
            WHERE {{
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14/prov/se/1>
                    AS ?s)
                ?s ?p ?o;
                    a <{ProvEntity.iri_entity}>.
            }}
        """
        output = {"head": {"vars": []}, "results": {"bindings": []}}
        output = Sparql(input_1, CONFIG)._get_results_from_files(output)
        # Sort bindings by p and o values for consistent comparison
        output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        expected_output = {
            "head": {"vars": ["p", "o"]},
            "results": {
                "bindings": [
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://purl.org/dc/terms/description",
                        },
                        "o": {
                            "type": "literal",
                            "value": "The entity "
                            "'https://github.com/arcangelo7/time_agnostic/id/14' "
                            "has been created.",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        },
                        "o": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/prov#Entity",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/prov#generatedAtTime",
                        },
                        "o": {"type": "literal", "value": "2021-05-07T09:59:15+00:00"},
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/prov#invalidatedAtTime",
                        },
                        "o": {"type": "literal", "value": "2021-06-01T18:46:41+00:00"},
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/prov#specializationOf",
                        },
                        "o": {
                            "type": "uri",
                            "value": "https://github.com/arcangelo7/time_agnostic/id/1"
                            "4",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/ns/prov#wasAttributedTo",
                        },
                        "o": {
                            "type": "uri",
                            "value": "https://orcid.org/0000-0002-8420-0696",
                        },
                    },
                ]
            },
        }
        # Sort expected bindings in the same way
        expected_output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        assert output == expected_output

    def test__get_tuples_from_triplestores(self):
        input_1 = """
            SELECT ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14> AS ?s)
                ?s ?p ?o
            }
        """
        output = {"head": {"vars": []}, "results": {"bindings": []}}
        output = Sparql(input_1, CONFIG)._get_results_from_triplestores(output)
        # Sort bindings by p and o values for consistent comparison
        output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        expected_output = {
            "head": {"vars": ["p", "o"]},
            "results": {
                "bindings": [
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://purl.org/spar/datacite/usesIdentifierSchem"
                            "e",
                        },
                        "o": {
                            "type": "uri",
                            "value": "http://purl.org/spar/datacite/orcid",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.essepuntato.it/2010/06/literalreificat"
                            "ion/hasLiteralValue",
                        },
                        "o": {
                            "type": "literal",
                            "value": "http://orcid.org/0000-0002-3259-2309",
                        },
                    },
                    {
                        "p": {
                            "type": "uri",
                            "value": "http://www.w3.org/1999/02/22-rdf-syntax-ns#type",
                        },
                        "o": {
                            "type": "uri",
                            "value": "http://purl.org/spar/datacite/Identifier",
                        },
                    },
                ]
            },
        }
        # Sort expected bindings in the same way
        expected_output["results"]["bindings"].sort(
            key=lambda x: (x["p"]["value"], x["o"]["value"])
        )
        assert output == expected_output

    def test_run_select_to_quad_set(self):
        query = """
            SELECT ?s ?p ?o ?c
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/id/14> as ?s)
                GRAPH ?c {?s ?p ?o}
            }
        """
        output = _to_nt_sorted_list(Sparql(query, CONFIG).run_select_to_quad_set())
        expected_output = [
            "<https://github.com/arcangelo7/time_agnostic/id/14> "
            "<http://purl.org/spar/datacite/usesIdentifierScheme> "
            "<http://purl.org/spar/datacite/orcid>",
            "<https://github.com/arcangelo7/time_agnostic/id/14> "
            "<http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue> "
            '"http://orcid.org/0000-0002-3259-2309"',
            "<https://github.com/arcangelo7/time_agnostic/id/14> "
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
            "<http://purl.org/spar/datacite/Identifier>",
        ]
        assert output == expected_output

    def test_run_select_to_quad_set_from_triplestore(self):
        query = """
            SELECT ?s ?p ?o
            WHERE {
                BIND (<https://github.com/arcangelo7/time_agnostic/ra/4> as ?s)
                ?s ?p ?o.
            }
        """
        output = _to_nt_sorted_list(Sparql(query, CONFIG).run_select_to_quad_set())
        expected_output = [
            "<https://github.com/arcangelo7/time_agnostic/ra/4> "
            "<http://purl.org/spar/datacite/hasIdentifier> "
            "<https://github.com/arcangelo7/time_agnostic/id/14>",
            "<https://github.com/arcangelo7/time_agnostic/ra/4> "
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> "
            "<http://xmlns.com/foaf/0.1/Agent>",
            "<https://github.com/arcangelo7/time_agnostic/ra/4> "
            '<http://xmlns.com/foaf/0.1/familyName> "Marini"',
            "<https://github.com/arcangelo7/time_agnostic/ra/4> "
            '<http://xmlns.com/foaf/0.1/givenName> "Giulio"',
            "<https://github.com/arcangelo7/time_agnostic/ra/4> "
            '<http://xmlns.com/foaf/0.1/name> "Giulio Marini"',
        ]
        assert output == expected_output
