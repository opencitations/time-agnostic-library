from time_agnostic_library.agnostic_query import VersionQuery, DeltaQuery
from time_agnostic_library.support import empty_the_cache
import rdflib
import unittest


CONFIG_PATH = "tests/config_fuseki.json"


class Test_Fuseki(unittest.TestCase):
    def test__get_query_to_update_queries_fuseki(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            SELECT DISTINCT ?a ?b
            WHERE {
                ?a literal:hasLiteralValue ?b.
            }
        """
        triple = (rdflib.term.Variable('a'), rdflib.term.URIRef('http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue'), rdflib.term.Variable('b'))
        query_to_identify = VersionQuery(query, config_path=CONFIG_PATH)._get_query_to_update_queries(triple).replace(" ", "").replace("\n", "")
        expected_query_to_identify = """
            PREFIX text: <http://jena.apache.org/text#>
            SELECT DISTINCT ?updateQuery WHERE {
                ?se text:query "\\"http://www.essepuntato.it/2010/06/literalreification/hasLiteralValue\\"";
                    <https://w3id.org/oc/ontology/hasUpdateQuery> ?updateQuery.
            }
        """.replace(" ", "").replace("\n", "")
        self.assertEqual(query_to_identify, expected_query_to_identify)

    def test_run_agnostic_query_easy(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-05-07T09:59:15': set(), '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}},
            set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_optional(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/4> rdf:type pro:RoleInTime.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}, '2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}, '2021-05-07T09:59:15': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}},
            set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_more_variables_and_more_optionals(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }, 
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }, 
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ra/4', 
                'https://github.com/arcangelo7/time_agnostic/id/14', 
                'http://orcid.org/0000-0002-3259-2309')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_obj_var(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?a ?id
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            },
            '2021-06-01T18:46:41': {
                (None, 'https://github.com/arcangelo7/time_agnostic/id/14')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_p_obj_var(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?s ?p
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?s ?p <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            },
            '2021-06-01T18:46:41': {(None, None)}
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_subj_obj_var(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            },
            '2021-06-01T18:46:41': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/4')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_var_multi_cont_values(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/2> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }   
        """
        agnostic_query = VersionQuery(query, other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': set(),
            '2021-05-30T16:42:28': set(),
            '2021-05-30T18:15:04': set(),
            '2021-05-30T19:41:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751')},
            '2021-05-30T20:28:47': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-30T21:29:54': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-30T23:37:34': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-31T20:31:01': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-05-31T21:55:56': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-06-01T01:02:01': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')},
            '2021-06-30T19:26:15': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751'),
                ('https://github.com/arcangelo7/time_agnostic/br/33757','https://github.com/arcangelo7/time_agnostic/id/27139','10.1007/s11192-006-0133-x')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_easy_on_time(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47", "2021-05-31T18:19:47"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-05-31T18:19:47': {('https://github.com/arcangelo7/time_agnostic/ra/15519',)}},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_easy_on_time_no_results(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o;
                    rdf:type pro:RoleInTime.
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-06", "2021-05-06"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            dict(), 
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_optional_on_time(self):
        query = """
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/4> rdf:type pro:RoleInTime.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-06-01T18:46:41", "2021-06-01T18:46:41"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = (
            {'2021-06-01T18:46:41': {('https://github.com/arcangelo7/time_agnostic/ra/4',)}},
            {'2021-05-31T18:19:47+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_more_variables_and_more_optionals_on_time(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            PREFIX pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            SELECT DISTINCT ?o ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ar/15519> pro:isHeldBy ?o.
                OPTIONAL {<https://github.com/arcangelo7/time_agnostic/ar/15519> rdf:type pro:RoleInTime.}
                ?o datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ra/15519', 
                'https://github.com/arcangelo7/time_agnostic/id/85509', 
                'http://orcid.org/0000-0002-3259-2309')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_obj_var_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?a ?id
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?a pro:isHeldBy <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519', 'https://github.com/arcangelo7/time_agnostic/id/14')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-31T18:19:47+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_p_obj_var_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            prefix datacite: <http://purl.org/spar/datacite/>
            prefix pro: <http://purl.org/spar/pro/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?s ?p
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/ra/4> datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?literalValue.
              OPTIONAL {?s ?p <https://github.com/arcangelo7/time_agnostic/ra/15519>.}
            }
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-07T09:59:15", "2021-05-07T09:59:15"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-07T09:59:15': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','http://purl.org/spar/pro/isHeldBy')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_subj_obj_var_on_time(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?s ?o
            WHERE {
                ?s pro:isHeldBy ?o.
            }        
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-31T18:19:47", "2021-05-31T18:19:47"), other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-31T18:19:47': {
                ('https://github.com/arcangelo7/time_agnostic/ar/15519','https://github.com/arcangelo7/time_agnostic/ra/15519')
            }},
            {'2021-06-01T18:46:41+00:00', '2021-05-07T09:59:15+00:00'})
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_var_multi_cont_values_on_time(self):
        query = """
            prefix literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/>
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/2> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                ?id literal:hasLiteralValue ?value.
            }   
        """
        agnostic_query = VersionQuery(query, on_time=("2021-05-30T19:41:57", "2021-05-30T19:41:57"), other_snapshots=False, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-05-30T19:41:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/31830','https://github.com/arcangelo7/time_agnostic/id/4','10.1080/15216540701258751')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_updating_relevant_times(self):
        query = """
            PREFIX literal: <http://www.essepuntato.it/2010/06/literalreification/>
            PREFIX cito: <http://purl.org/spar/cito/>
            PREFIX datacite: <http://purl.org/spar/datacite/> 
            SELECT DISTINCT ?br ?id ?value
            WHERE {
                <https://github.com/arcangelo7/time_agnostic/br/69211> cito:cites ?br.
                ?br datacite:hasIdentifier ?id.
                OPTIONAL {?id literal:hasLiteralValue ?value.}
            }   
        """
        agnostic_query = VersionQuery(query, other_snapshots=True, config_path=CONFIG_PATH)
        output = agnostic_query.run_agnostic_query()
        expected_output = ({
            '2021-09-09T14:34:43': set(), 
            '2021-09-13T16:42:27': set(), 
            '2021-09-13T16:43:22': set(), 
            '2021-09-13T16:45:30': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:47:44': {
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:48:32': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:50:06': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None)}, 
            '2021-09-13T16:51:57': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)}, 
            '2021-09-13T17:08:13': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)}, 
            '2021-09-13T17:09:28': {
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)}, 
            '2021-09-13T17:10:27': {
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None)}, 
            '2021-09-13T17:11:16': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', None), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')}, 
            '2021-09-13T17:11:59': {
                ('https://github.com/arcangelo7/time_agnostic/br/528724', 'https://github.com/arcangelo7/time_agnostic/id/282400', '10.1073/pnas.0507655102'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528726', 'https://github.com/arcangelo7/time_agnostic/id/282402', '10.1111/j.1365-2702.2009.02927.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528728', 'https://github.com/arcangelo7/time_agnostic/id/282404', '10.1111/j.1365-2702.2010.03679.x'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528725', 'https://github.com/arcangelo7/time_agnostic/id/282401', '10.3109/10673229.2010.493742'), 
                ('https://github.com/arcangelo7/time_agnostic/br/528727', 'https://github.com/arcangelo7/time_agnostic/id/282403', '10.3928/00220124-20100126-03')
            }
        }, set())
        self.assertEqual(output, expected_output)

    def test_run_agnostic_query_cross_delta(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": "2021-05-07T09:59:15",
                "modified": {
                    '2021-06-01T18:46:41': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_before(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = (None, "2021-06-02T18:46:41")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": "2021-05-07T09:59:15",
                "modified": {
                    '2021-06-01T18:46:41': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_single_delta_interval(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = ("2021-06-01", "2021-06-02T18:46:41")
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ar/15519': {
                "created": None,
                "modified": {
                    '2021-06-01T18:46:41': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/15519> .} }; INSERT DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ar/> { <https://github.com/arcangelo7/time_agnostic/ar/15519> <http://purl.org/spar/pro/isHeldBy> <https://github.com/arcangelo7/time_agnostic/ra/4> .} }'
                },
                "deleted": None
            }
        }
        self.assertEqual(agnostic_results, expected_output)


    def test_run_agnostic_query_single_delta_after_no_results(self):
        query = """
            prefix pro: <http://purl.org/spar/pro/>
            SELECT DISTINCT ?ar
            WHERE {
                ?ar a pro:RoleInTime. 
            }
        """
        on_time = ("2021-06-02", None)
        changed_properties = {"http://purl.org/spar/pro/isHeldBy"}   
        delta_query = DeltaQuery(query=query, on_time=on_time, changed_properties=changed_properties, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = dict()
        self.assertEqual(agnostic_results, expected_output)

    def test_run_agnostic_query_on_deleted_entity(self):
        query = """
            prefix foaf: <http://xmlns.com/foaf/0.1/>
            SELECT DISTINCT ?ra
            WHERE {
                ?ra a foaf:Agent. 
            }
        """
        delta_query = DeltaQuery(query=query, config_path=CONFIG_PATH)
        agnostic_results = delta_query.run_agnostic_query()
        expected_output = {
            'https://github.com/arcangelo7/time_agnostic/ra/15519': {
                'created': '2021-05-07T09:59:15',
                'deleted': '2021-06-01T18:46:41',
                'modified': {
                    '2021-06-01T18:46:41': 'DELETE DATA { GRAPH <https://github.com/arcangelo7/time_agnostic/ra/> { <https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/name> "Giulio Marini"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/givenName> "Giulio"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://purl.org/spar/datacite/hasIdentifier> <https://github.com/arcangelo7/time_agnostic/id/85509> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://xmlns.com/foaf/0.1/familyName> "Marini"^^<http://www.w3.org/2001/XMLSchema#string> .\n<https://github.com/arcangelo7/time_agnostic/ra/15519> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://xmlns.com/foaf/0.1/Agent> .} }'
                }   
            },
            'https://github.com/arcangelo7/time_agnostic/ra/4': {
                'created': '2021-05-07T09:59:15',
                'deleted': None,
                'modified': {
                    '2021-06-01T18:46:41': "The entity 'https://github.com/arcangelo7/time_agnostic/ra/4' has been merged with 'https://github.com/arcangelo7/time_agnostic/ra/15519'."
                }
            }      
        }
        self.assertEqual(agnostic_results, expected_output)


if __name__ == '__main__':
    unittest.main()