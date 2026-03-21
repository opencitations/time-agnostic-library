#!/usr/bin/python

# SPDX-FileCopyrightText: 2016 Silvio Peroni <essepuntato@gmail.com>
# SPDX-FileCopyrightText: 2021-2026 Arcangelo Massari <arcangelo.massari@unibo.it>
#
# SPDX-License-Identifier: ISC

from typing import ClassVar


class ProvEntity:
    PROV: ClassVar[str] = "http://www.w3.org/ns/prov#"
    DCTERMS: ClassVar[str] = "http://purl.org/dc/terms/"
    OCO: ClassVar[str] = "https://w3id.org/oc/ontology/"

    iri_entity: ClassVar[str] = PROV + "Entity"
    iri_generated_at_time: ClassVar[str] = PROV + "generatedAtTime"
    iri_invalidated_at_time: ClassVar[str] = PROV + "invalidatedAtTime"
    iri_specialization_of: ClassVar[str] = PROV + "specializationOf"
    iri_was_derived_from: ClassVar[str] = PROV + "wasDerivedFrom"
    iri_had_primary_source: ClassVar[str] = PROV + "hadPrimarySource"
    iri_was_attributed_to: ClassVar[str] = PROV + "wasAttributedTo"
    iri_description: ClassVar[str] = DCTERMS + "description"
    iri_has_update_query: ClassVar[str] = OCO + "hasUpdateQuery"

    _prov_properties: ClassVar[tuple[str, ...]] = ()

    @classmethod
    def get_prov_properties(cls) -> tuple[str, ...]:
        if not cls._prov_properties:
            cls._prov_properties = (
                cls.iri_entity, cls.iri_generated_at_time, cls.iri_invalidated_at_time,
                cls.iri_specialization_of, cls.iri_was_derived_from, cls.iri_had_primary_source,
                cls.iri_was_attributed_to, cls.iri_description, cls.iri_has_update_query
            )
        return cls._prov_properties
