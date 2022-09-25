from sys import platform
from subprocess import Popen
import json
import os
import requests
import wget
import zipfile


BASE_DIR = os.path.abspath('tests')
is_unix = platform == 'darwin' or platform.startswith('linux')
if is_unix:
    from signal import SIGKILL
    from subprocess import PIPE
else:
    from subprocess import CREATE_NEW_CONSOLE

CREATE_NEW_CONSOLE = CREATE_NEW_CONSOLE if not is_unix else 0

def download_tests_datasets_from_zenodo():
    url = 'https://zenodo.org/api/records/6539398#.YnvKljlByrk'
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
    graphdb_bin = 'graphdb' if is_unix else 'graphdb.cmd'
    path_to_graphdb = f'{BASE_DIR}/graphdb/bin/{graphdb_bin}'
    Popen(
        [path_to_graphdb, f'-Dgraphdb.connector.port={port}'],
        creationflags=CREATE_NEW_CONSOLE
    )

def launch_fuseki(port:int=3030):
    '''
    Launch Fuseki triplestore at a given port.
    '''
    fuseki_bin = 'fuseki-server' if is_unix else 'fuseki-server.bat'
    path_to_fuseki = f'{BASE_DIR}/fuseki/{fuseki_bin}'
    os.environ['FUSEKI_BASE'] = f'{BASE_DIR}/fuseki'
    os.environ['FUSEKI_HOME'] = f'{BASE_DIR}/fuseki'
    Popen(
        [path_to_fuseki, f'--port={str(port)}'],
        creationflags=CREATE_NEW_CONSOLE
    )

def launch_virtuoso(ts_dir:str):
    '''
    Launch Virtuoso triplestore.
    '''

    if os.path.exists(f'{ts_dir}/virtuoso.lck'):
        os.remove(f'{ts_dir}/virtuoso.lck')
    virtuoso_platform = 'linux' if is_unix else 'windows'
    virtuoso_bin = 'virtuoso-t' if is_unix else 'virtuoso-t.exe'
    Popen(
        [f'{BASE_DIR}/virtuoso/{virtuoso_platform}/bin/{virtuoso_bin}', '-f', '-c', f'{ts_dir}/virtuoso.ini'],
        creationflags=CREATE_NEW_CONSOLE
    )

if __name__ == '__main__':
    if not os.path.isfile(f'{BASE_DIR}/blazegraph.jnl'):
        download_tests_datasets_from_zenodo()
        unzip(f'{BASE_DIR}/tests.zip', f'{BASE_DIR}/')
    if is_unix:
        for port in [9999, 29999, 7200, 3030, 8890, 8891, 1111, 1112]:
            process = Popen(["lsof", "-i", ":{0}".format(port)], stdout=PIPE, stderr=PIPE)
            stdout, _ = process.communicate()
            for process in str(stdout.decode("utf-8")).split("\n")[1:]:
                data = [x for x in process.split(" ") if x != '']
                if (len(data) <= 1):
                    continue
                os.kill(int(data[1]), SIGKILL)
        Popen(['chmod', '+x', f'{BASE_DIR}/graphdb/bin/graphdb', f'{BASE_DIR}/fuseki/fuseki-server', f'{BASE_DIR}/virtuoso/linux/bin/virtuoso-t'])
    launch_blazegraph('tests', 9999)
    launch_blazegraph(f'{BASE_DIR}/cache', 29999)
    launch_graphdb(7200)
    launch_fuseki(3030)
    virtuoso_platform = 'linux' if is_unix else 'windows'
    launch_virtuoso(f'{BASE_DIR}/virtuoso/{virtuoso_platform}/database')
    launch_virtuoso(f'{BASE_DIR}/virtuoso/{virtuoso_platform}/cache_db')