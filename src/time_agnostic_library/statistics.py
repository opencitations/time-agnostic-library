#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2022, Arcangelo Massari <arcangelo.massari@unibo.it>
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


from typing import Union
from time_agnostic_library.agnostic_entity import AgnosticEntity


class Statistics:
    def __init__(self, snapshots:Union[dict,tuple]):
        if type(snapshots) is tuple:
            entity_snapshots, other_snapshots = snapshots
            entity_snapshots = sorted([AgnosticEntity._convert_to_datetime(data['generatedAtTime']) for _, data in entity_snapshots.items()], reverse=True)
            other_snapshots = [data['generatedAtTime'] for _, data in other_snapshots.items() if AgnosticEntity._convert_to_datetime(data['generatedAtTime']) >= entity_snapshots[0]]
            self.snapshots = {'entity': len(entity_snapshots + other_snapshots)}
        else:
            self.snapshots = {entity:len(se) for entity,se in snapshots.items()}
        
    def get_number_of_snapshots(self):
        return sum(se for _, se in self.snapshots.items())

    def get_number_of_entities(self):
        return len(self.snapshots)

    
