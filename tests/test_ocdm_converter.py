#!/usr/bin/python
# -*- coding: utf-8 -*-
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

import gzip
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from time_agnostic_library.ocdm_converter import (
    OCDMConverter,
    _build_update_query,
    _escape_sparql_for_nquads,
    _format_timestamp,
    _read_and_group,
    extract_subject_uri,
    group_triples_by_subject,
    parse_ntriples_line,
    read_ntriples_file,
)


class TestParseNtriplesLine(unittest.TestCase):
    def test_uri_triple(self):
        line = '<http://example.com/s> <http://example.com/p> <http://example.com/o> .'
        self.assertEqual(
            parse_ntriples_line(line),
            ('<http://example.com/s>', '<http://example.com/p>', '<http://example.com/o>'),
        )

    def test_literal_with_datatype(self):
        line = '<http://example.com/s> <http://example.com/p> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertEqual(result[0], '<http://example.com/s>')
        self.assertEqual(result[1], '<http://example.com/p>')
        self.assertIn('42', result[2])

    def test_literal_with_lang_tag(self):
        line = '<http://example.com/s> <http://example.com/p> "hello"@en .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertEqual(result[2], '"hello"@en')

    def test_plain_literal(self):
        line = '<http://example.com/s> <http://example.com/p> "plain text" .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertEqual(result[2], '"plain text"')

    def test_blank_node_subject(self):
        line = '_:b0 <http://example.com/p> <http://example.com/o> .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertEqual(result[0], '_:b0')

    def test_blank_node_object(self):
        line = '<http://example.com/s> <http://example.com/p> _:b1 .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertEqual(result[2], '_:b1')

    def test_comment_line(self):
        self.assertIsNone(parse_ntriples_line('# This is a comment'))

    def test_empty_line(self):
        self.assertIsNone(parse_ntriples_line(''))
        self.assertIsNone(parse_ntriples_line('   '))

    def test_object_normalizer(self):
        line = '<http://example.com/s> <http://example.com/p> "value" .'
        normalizer = lambda obj: obj.upper()
        result = parse_ntriples_line(line, object_normalizer=normalizer)
        assert result is not None
        self.assertEqual(result[2], '"VALUE"')

    def test_escaped_literal(self):
        line = r'<http://example.com/s> <http://example.com/p> "line1\nline2" .'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertIn('line1', result[2])


class TestParseNtriplesLineFallback(unittest.TestCase):
    def test_non_uri_datatype_fallback(self):
        line = '<http://s> <http://p> "42"^^xsd:integer .'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"42"^^xsd:integer'))

    def test_no_trailing_dot(self):
        line = '<http://s> <http://p> <http://o>'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '<http://o>'))

    def test_fallback_literal_with_datatype_uri(self):
        line = '<http://s> <http://p> "val"^^<http://type>'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"val"^^<http://type>'))

    def test_fallback_literal_with_lang_tag(self):
        line = '<http://s> <http://p> "hello"@en'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"hello"@en'))

    def test_fallback_plain_literal(self):
        line = '<http://s> <http://p> "text"'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"text"'))

    def test_fallback_blank_node(self):
        line = '_:b0 <http://p> _:b1'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('_:b0', '<http://p>', '_:b1'))

    def test_fallback_escaped_literal(self):
        line = r'<http://s> <http://p> "line1\"quoted"'
        result = parse_ntriples_line(line)
        assert result is not None
        self.assertIn('quoted', result[2])

    def test_fallback_with_normalizer(self):
        line = '<http://s> <http://p> "value"'
        normalizer = lambda obj: obj.upper()
        result = parse_ntriples_line(line, object_normalizer=normalizer)
        assert result is not None
        self.assertEqual(result[2], '"VALUE"')

    def test_fallback_incomplete_line(self):
        line = '<http://s> <http://p>'
        result = parse_ntriples_line(line)
        self.assertIsNone(result)

    def test_fallback_dot_no_space(self):
        line = '<http://s> <http://p> <http://o>.'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '<http://o>'))

    def test_fallback_tab_separator(self):
        line = '<http://s>\t<http://p>\t<http://o>'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '<http://o>'))

    def test_fallback_dot_no_space_non_uri_dtype(self):
        line = '<http://s> <http://p> "42"^^xsd:integer.'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"42"^^xsd:integer'))

    def test_fallback_non_uri_datatype_with_trailing(self):
        line = '<http://s> <http://p> "42"^^xsd:integer extra'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"42"^^xsd:integer'))

    def test_fallback_non_uri_datatype_no_space(self):
        line = '<http://s> <http://p> "42"^^xsd:integer'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"42"^^xsd:integer'))

    def test_fallback_lang_tag_with_trailing(self):
        line = '<http://s> <http://p> "hello"@en extra'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"hello"@en'))

    def test_fallback_unknown_chars(self):
        line = 'foo <http://p> bar'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('foo', '<http://p>', 'bar'))

    def test_fallback_lang_tag_no_space(self):
        line = '<http://s> <http://p> "ciao"@it'
        result = parse_ntriples_line(line)
        self.assertEqual(result, ('<http://s>', '<http://p>', '"ciao"@it'))


class TestExtractSubjectUri(unittest.TestCase):
    def test_angle_bracket_uri(self):
        self.assertEqual(extract_subject_uri('<http://example.com/s>'), 'http://example.com/s')

    def test_blank_node(self):
        self.assertEqual(extract_subject_uri('_:b0'), '_:b0')


class TestReadNtriplesFile(unittest.TestCase):
    def test_read_plain_file(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.nt', delete=False) as f:
            f.write('<http://example.com/s1> <http://example.com/p> <http://example.com/o1> .\n')
            f.write('# comment\n')
            f.write('<http://example.com/s2> <http://example.com/p> "text" .\n')
            path = Path(f.name)
        result = read_ntriples_file(path)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0][0], '<http://example.com/s1>')
        self.assertEqual(result[1][2], '"text"')
        path.unlink()


    def test_read_ntriples_file_gzip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'test.nt.gz'
            with gzip.open(path, 'wt', encoding='utf-8') as f:
                f.write('<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n')
            result = read_ntriples_file(path)
            self.assertEqual(result, [('<http://example.com/s>', '<http://example.com/p>', '<http://example.com/o>')])


class TestGroupTriplesBySubject(unittest.TestCase):
    def test_grouping(self):
        triples = [
            ('<http://example.com/s1>', '<http://example.com/p1>', '"a"'),
            ('<http://example.com/s1>', '<http://example.com/p2>', '"b"'),
            ('<http://example.com/s2>', '<http://example.com/p1>', '"c"'),
        ]
        result = group_triples_by_subject(triples)
        self.assertEqual(
            result['http://example.com/s1'],
            {('<http://example.com/p1>', '"a"'), ('<http://example.com/p2>', '"b"')},
        )
        self.assertEqual(
            result['http://example.com/s2'],
            {('<http://example.com/p1>', '"c"')},
        )


class TestReadAndGroup(unittest.TestCase):
    def test_with_normalizer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'test.nt'
            path.write_text(
                '<http://example.com/s1> <http://example.com/p> "abc" .\n'
                '<http://example.com/s2> <http://example.com/p> "42"^^xsd:integer\n'
            )
            normalizer = lambda obj: obj.upper()
            result = _read_and_group(path, normalizer)
            self.assertEqual(result['http://example.com/s1'], {('<http://example.com/p>', '"ABC"')})
            self.assertEqual(result['http://example.com/s2'], {('<http://example.com/p>', '"42"^^XSD:INTEGER')})


    def test_gzip(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'test.nt.gz'
            with gzip.open(path, 'wt', encoding='utf-8') as f:
                f.write('<http://example.com/s> <http://example.com/p> <http://example.com/o> .\n')
            result = _read_and_group(path)
            self.assertEqual(result['http://example.com/s'], {('<http://example.com/p>', '<http://example.com/o>')})

    def test_fallback_no_normalizer(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / 'test.nt'
            path.write_text('<http://example.com/s> <http://example.com/p> "42"^^xsd:integer\n')
            result = _read_and_group(path)
            self.assertEqual(result['http://example.com/s'], {('<http://example.com/p>', '"42"^^xsd:integer')})


class TestBuildUpdateQuery(unittest.TestCase):
    def test_delete_and_insert(self):
        result = _build_update_query(
            'http://example.com/s',
            'http://example.com/graph/',
            {('<http://example.com/p>', '"old"')},
            {('<http://example.com/p>', '"new"')},
        )
        self.assertIn('DELETE DATA', result)
        self.assertIn('INSERT DATA', result)
        self.assertIn('; ', result)

    def test_delete_only(self):
        result = _build_update_query(
            'http://example.com/s',
            'http://example.com/graph/',
            {('<http://example.com/p>', '"old"')},
            set(),
        )
        self.assertIn('DELETE DATA', result)
        self.assertNotIn('INSERT DATA', result)

    def test_insert_only(self):
        result = _build_update_query(
            'http://example.com/s',
            'http://example.com/graph/',
            set(),
            {('<http://example.com/p>', '"new"')},
        )
        self.assertNotIn('DELETE DATA', result)
        self.assertIn('INSERT DATA', result)


class TestEscapeSparql(unittest.TestCase):
    def test_escapes(self):
        result = _escape_sparql_for_nquads('a"b\nc\\d\r\t')
        self.assertEqual(result, 'a\\"b\\nc\\\\d\\r\\t')


class TestFormatTimestamp(unittest.TestCase):
    def test_format(self):
        dt = datetime(2021, 5, 7, 9, 59, 15)
        self.assertEqual(_format_timestamp(dt), '2021-05-07T09:59:15+00:00')


class TestOCDMConverterIC(unittest.TestCase):
    def test_convert_from_ic(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            v1 = tmp / 'v1.nt'
            v2 = tmp / 'v2.nt'
            v3 = tmp / 'v3.nt'
            v1.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1" .\n'
                '<http://example.com/e1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/Type> .\n'
                '<http://example.com/e2> <http://example.com/p> "value2" .\n'
            )
            v2.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1_updated" .\n'
                '<http://example.com/e1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/Type> .\n'
                '<http://example.com/e2> <http://example.com/p> "value2" .\n'
            )
            v3.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1_updated" .\n'
                '<http://example.com/e1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://example.com/Type> .\n'
            )

            timestamps = [
                datetime(2021, 5, 7, 9, 0, 0),
                datetime(2021, 5, 8, 9, 0, 0),
                datetime(2021, 5, 9, 9, 0, 0),
            ]
            dataset_out = tmp / 'dataset.nq'
            prov_out = tmp / 'provenance.nq'

            converter = OCDMConverter('http://example.com/graph/', 'http://example.com/agent')
            converter.convert_from_ic([v1, v2, v3], timestamps, dataset_out, prov_out)

            dataset_text = dataset_out.read_text()
            self.assertIn('<http://example.com/e1>', dataset_text)
            self.assertIn('value1_updated', dataset_text)
            self.assertNotIn('<http://example.com/e2>', dataset_text)

            prov_text = prov_out.read_text()
            self.assertIn('specializationOf', prov_text)
            self.assertIn('generatedAtTime', prov_text)
            self.assertIn('hasUpdateQuery', prov_text)
            self.assertIn('wasDerivedFrom', prov_text)
            self.assertIn('invalidatedAtTime', prov_text)
            self.assertIn('The entity has been created.', prov_text)
            self.assertIn('The entity has been modified.', prov_text)

            entities_in_data = {'http://example.com/e1', 'http://example.com/e2'}
            for entity in entities_in_data:
                self.assertIn(f'<{entity}/prov/se/1>', prov_text)


class TestOCDMConverterCB(unittest.TestCase):
    def test_convert_from_cb(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            initial = tmp / 'initial.nt'
            initial.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1" .\n'
                '<http://example.com/e2> <http://example.com/p> "value2" .\n'
            )
            added1 = tmp / 'added1.nt'
            added1.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1_new" .\n'
            )
            deleted1 = tmp / 'deleted1.nt'
            deleted1.write_text(
                '<http://example.com/e1> <http://example.com/p> "value1" .\n'
            )
            added2 = tmp / 'added2.nt'
            added2.write_text('')
            deleted2 = tmp / 'deleted2.nt'
            deleted2.write_text(
                '<http://example.com/e2> <http://example.com/p> "value2" .\n'
            )

            timestamps = [
                datetime(2021, 5, 7, 9, 0, 0),
                datetime(2021, 5, 8, 9, 0, 0),
                datetime(2021, 5, 9, 9, 0, 0),
            ]
            dataset_out = tmp / 'dataset.nq'
            prov_out = tmp / 'provenance.nq'

            converter = OCDMConverter('http://example.com/graph/', 'http://example.com/agent')
            converter.convert_from_cb(
                initial, [(added1, deleted1), (added2, deleted2)],
                timestamps, dataset_out, prov_out,
            )

            dataset_text = dataset_out.read_text()
            self.assertIn('value1_new', dataset_text)
            self.assertNotIn('value2', dataset_text)

            prov_text = prov_out.read_text()
            self.assertIn('specializationOf', prov_text)
            self.assertIn('invalidatedAtTime', prov_text)


if __name__ == '__main__':
    unittest.main()
