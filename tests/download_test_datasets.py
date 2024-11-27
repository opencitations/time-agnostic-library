import json
import os
import zipfile
from subprocess import Popen
from sys import platform

import requests
import wget

BASE_DIR = os.path.abspath('tests')
is_unix = platform == 'darwin' or platform.startswith('linux')
if is_unix:
    from signal import SIGKILL
    from subprocess import PIPE
else:
    from subprocess import CREATE_NEW_CONSOLE

CREATE_NEW_CONSOLE = CREATE_NEW_CONSOLE if not is_unix else 0

def download_tests_datasets_from_zenodo():
    url = 'https://zenodo.org/api/records/14230701'
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

if __name__ == '__main__': # pragma: no cover
    if not os.path.isfile(f'{BASE_DIR}/blazegraph.jnl'):
        download_tests_datasets_from_zenodo()
        unzip(f'{BASE_DIR}/tests.zip', f'{BASE_DIR}/')
    if is_unix:
        for port in [9999]:
            process = Popen(["lsof", "-i", ":{0}".format(port)], stdout=PIPE, stderr=PIPE)
            stdout, _ = process.communicate()
            for process in str(stdout.decode("utf-8")).split("\n")[1:]:
                data = [x for x in process.split(" ") if x != '']
                if (len(data) <= 1):
                    continue
                os.kill(int(data[1]), SIGKILL)
    launch_blazegraph('tests', 9999)