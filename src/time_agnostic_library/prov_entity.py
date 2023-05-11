#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2016, Silvio Peroni <essepuntato@gmail.com>
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

from typing import ClassVar

from rdflib import Namespace, URIRef


class ProvEntity:
    """Snapshot of entity metadata: a particular snapshot recording the
    metadata associated with an individual entity (either a bibliographic entity or an
    identifier) at a particular date and time, including the agent, such as a person,
    organisation or automated process that created or modified the entity metadata.
    """

    PROV: ClassVar[Namespace] = Namespace("http://www.w3.org/ns/prov#")
    DCTERMS: ClassVar[Namespace] = Namespace("http://purl.org/dc/terms/")
    OCO: ClassVar[Namespace] = Namespace("https://w3id.org/oc/ontology/")

    iri_entity: ClassVar[URIRef] = PROV.Entity
    iri_generated_at_time: ClassVar[URIRef] = PROV.generatedAtTime
    iri_invalidated_at_time: ClassVar[URIRef] = PROV.invalidatedAtTime
    iri_specialization_of: ClassVar[URIRef] = PROV.specializationOf
    iri_was_derived_from: ClassVar[URIRef] = PROV.wasDerivedFrom
    iri_had_primary_source: ClassVar[URIRef] = PROV.hadPrimarySource
    iri_was_attributed_to: ClassVar[URIRef] = PROV.wasAttributedTo
    iri_description: ClassVar[URIRef] = DCTERMS.description
    iri_has_update_query: ClassVar[URIRef] = OCO.hasUpdateQuery

    @classmethod
    def get_prov_properties(cls):
        prov_properties = [
            cls.iri_entity, cls.iri_generated_at_time, cls.iri_invalidated_at_time, 
            cls.iri_specialization_of, cls.iri_was_derived_from, cls.iri_had_primary_source, 
            cls.iri_was_attributed_to, cls.iri_description, cls.iri_has_update_query
        ]
        return prov_properties