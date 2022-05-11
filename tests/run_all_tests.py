from sys import platform
from subprocess import CREATE_NEW_CONSOLE, Popen
import json
import os
import requests
import time
import wget
import zipfile

BASE_DIR = os.path.abspath('tests')
is_unix = platform == 'darwin' or platform.startswith('linux')
CREATE_NEW_CONSOLE = CREATE_NEW_CONSOLE if not is_unix else 0

def download_tests_datasets_from_zenodo():
    url = 'https://zenodo.org/api/records/6408368#.YkhYoChBxaZ'
    r = requests.get(url)
    if r.ok:
        js = json.loads(r.text)
        files = js['files']
        for f in files:
            link = f['links']['self']
            wget.download(url=link, out=BASE_DIR)

def unzip(file:str, destination:str):
    '''
    Unzip the file to the destination directory.
    '''
    with zipfile.ZipFile(file, 'r') as zip_ref:
        zip_ref.extractall(destination)

def launch_blazegraph(ts_dir:str, port:int):
    '''
    Launch Blazegraph triplestore at a given port.
    '''
    Popen(
        ['java', '-server', '-Xmx4g', F'-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl',f'-Djetty.port={port}', '-jar', f'{ts_dir}/blazegraph.jar'],
        creationflags=CREATE_NEW_CONSOLE
    )

def launch_graphdb(port:int=7200):
    '''
    Launch GraphDB triplestore at a given port.
    '''
    abs_path_to_graphdb = os.path.abspath(f'{BASE_DIR}/graphdb/bin/graphdb.cmd')
    Popen(
        [abs_path_to_graphdb, f'-Dgraphdb.connector.port={port}'],
        creationflags=CREATE_NEW_CONSOLE
    )

def launch_fuseki(port:int=3030):
    '''
    Launch Fuseki triplestore at a given port.
    '''
    abs_path_to_graphdb = os.path.abspath(f'{BASE_DIR}/fuseki/fuseki-server.bat')
    os.environ['FUSEKI_BASE'] = os.path.abspath(f'{BASE_DIR}/fuseki')
    os.environ['FUSEKI_HOME'] = os.path.abspath(f'{BASE_DIR}/fuseki')
    Popen(
        [abs_path_to_graphdb, f'--port={str(port)}'],
        creationflags=CREATE_NEW_CONSOLE
    )

def launch_virtuoso(ts_dir:str):
    '''
    Launch Virtuoso triplestore.
    '''
    if os.path.exists(f'{ts_dir}/virtuoso.lck'):
        os.remove(f'{ts_dir}/virtuoso.lck')
    Popen(
        [f'{BASE_DIR}/virtuoso/bin/virtuoso-t.exe', '-f', '-c', f'{ts_dir}/virtuoso.ini'],
        creationflags=CREATE_NEW_CONSOLE
    )

def main():
    if not os.path.isfile(f'{BASE_DIR}/blazegraph.jnl'):
        download_tests_datasets_from_zenodo()
        unzip(f'{BASE_DIR}/tests.zip', f'{BASE_DIR}/')
    launch_blazegraph('tests', 9999)
    launch_blazegraph(f'{BASE_DIR}/cache', 29999)
    launch_graphdb(7200)
    launch_fuseki(3030)
    launch_virtuoso(f'{BASE_DIR}/virtuoso/database')
    launch_virtuoso(f'{BASE_DIR}/virtuoso/cache_db')
    time.sleep(10)
    Popen(
        ['python', '-m', 'unittest', 'discover', '-s', 'tests', '-p', 'test*.py', '-b']
    )