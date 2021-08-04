#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (c) 2021, Arcangelo Massari <arcangelomas@gmail.com>
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

from pprint import pprint
from typing import Dict, List

import json, os, zipfile
from zipfile import ZipFile
from rdflib.graph import ConjunctiveGraph


class FileManager:
    """
    Convenient class for file management.

    :param path: The path to a file.
    :type path: str.
    """
    def __init__(self, path:str):
        self.path = path
        
    def import_json(self) -> dict:
        """
        Import a JSON file as a dictionary.

        :returns: dict -- A dictionary representing the imported JSON file.
        """
        with open(self.path, encoding="utf8") as json_file:
            return json.load(json_file)
    
    def dump_json(self, json_data:dict, beautiful:bool=False) -> None:
        with open(self.path, 'w') as outfile:
            print(f"[FileManager: INFO] Writing json to path {self.path}")
            if beautiful:
                json.dump(json_data, outfile, sort_keys=True, indent=4)
            else:
                json.dump(json_data, outfile)

    def _zipdir(self, ziph:ZipFile) -> None:
        for root, dirs, files in os.walk(self.path):
            dirs[:] = [d for d in dirs if d != "small"]
            for file in files:
                ziph.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), os.path.join(self.path, '..')))
    
    def zip_data(self) -> None:
        """
        Zip a file as "output.zip" to the script execution path.

        :returns: None -- The output can be found under the name "output.zip" in the execution path of the Python script.
        """
        zipf = zipfile.ZipFile('output.zip', 'w', zipfile.ZIP_DEFLATED)
        self._zipdir(self.path, zipf)
        zipf.close()

    def minify_json(self) -> None:
        """
        Minify a JSON file.

        :returns: None -- The output can be found under the same name of the input + "_min.json" in the same folder of the input file.
        """
        print(f"[FileManager: INFO] Minifing file {self.path}")
        file_data = open(self.path, "r", encoding="utf-8").read()
        json_data = json.loads(file_data) 
        json_string = json.dumps(json_data, separators=(',', ":")) 
        path = str(self.path).replace(".json", "")
        new_path = "{0}_min.json".format(path)
        open(new_path, "w+", encoding="utf-8").write(json_string) 

def _to_nt_sorted_list(cg:ConjunctiveGraph) -> list:
    if cg is None:
        return None
    nt_list = str(cg.serialize(format="nt")).split(r".\n")
    sorted_nt_list = sorted([triple.replace("b\'", "").strip() for triple in nt_list if triple != r"b'\n'" and triple != r"\n'"])
    return sorted_nt_list

def _to_dict_of_nt_sorted_lists(dictionary:Dict[str, Dict[str, ConjunctiveGraph]]) -> Dict[str, Dict[str, List[str]]]:
    dict_of_nt_sorted_lists = dict()
    for key, value in dictionary.items():
        if isinstance(value, ConjunctiveGraph):
            dict_of_nt_sorted_lists[key] = _to_nt_sorted_list(value)
        else:
            for snapshot, cg in value.items():
                dict_of_nt_sorted_lists.setdefault(key, dict())
                dict_of_nt_sorted_lists[key][snapshot] = _to_nt_sorted_list(cg)
    return dict_of_nt_sorted_lists

def _to_conjunctive_graph(nt_list:List[str]) -> ConjunctiveGraph:
    cg = ConjunctiveGraph()
    for triple in nt_list:
        cg.parse(data=triple + ".", format="nt")
    return cg

def _to_dict_of_conjunctive_graphs(dictionary:Dict[str, Dict[str, List]]) -> Dict[str, Dict[str, ConjunctiveGraph]]:
    dict_of_conjunctive_graphs = dict()
    for key, value in dictionary.items():
        if isinstance(value, list):
            cg = _to_conjunctive_graph(value)
            dict_of_conjunctive_graphs.setdefault(key, dict())
            dict_of_conjunctive_graphs[key] = cg
        else:
            for snapshot, triples in value.items():
                cg = _to_conjunctive_graph(triples)
                dict_of_conjunctive_graphs.setdefault(key, dict())
                dict_of_conjunctive_graphs[key][snapshot] = cg
    return dict_of_conjunctive_graphs










