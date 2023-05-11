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
from time_agnostic_library.support import convert_to_datetime


class Statistics:
    def __init__(self, data:Union[dict,tuple]):
        self.data = data

    def get_overhead(self):
        if type(self.data) is tuple:
            entity_snapshots, other_snapshots = self.data
            if entity_snapshots:
                entity_snapshots = sorted([convert_to_datetime(data['generatedAtTime']) for _, data in entity_snapshots.items()], reverse=True)
                other_snapshots = [data['generatedAtTime'] for _, data in other_snapshots.items() if convert_to_datetime(data['generatedAtTime']) >= entity_snapshots[0]]
                return len(entity_snapshots + other_snapshots)
            else:
                return 0
        elif type(self.data) is dict:
            return sum(len(se) for _, se in self.data.items())
        elif type(self.data) is set:
            return len(self.data)  
