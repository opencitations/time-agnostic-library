from setup_and_tests import get_setup
from subprocess import Popen, PIPE
from sys import platform
if not platform == 'darwin' and not platform.startswith('linux'):
    from subprocess import CREATE_NEW_CONSOLE
from time import sleep
from time_agnostic_library.support import empty_the_cache
import os
import psutil
import signal


class TriplestoreManager:
    def __init__(self):
        self.is_unix = platform == 'darwin' or platform.startswith('linux')

    def close_program_by_name(self, name):
        pids = self.__find_pid_by_name(name)
        for pid in pids:
            if self.is_unix:
                os.kill(pid, signal.SIGKILL)
            else:
                os.kill(pid, signal.SIGTERM)

    def __find_pid_by_name(self, processName):
        listOfProcessObjects = []
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=['pid', 'name'])
                # Check if process name contains the given name string.
                if processName.lower() in pinfo['name'].lower():
                    listOfProcessObjects.append(pinfo['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied , psutil.ZombieProcess):
                pass
        return listOfProcessObjects

    def launch_blazegraph(self, ts_dir:str, port:int):
        '''
        Launch Blazegraph triplestore at a given port.
        '''
        if self.is_unix:
            Popen(
                ['gnome-terminal', '--', 'java', '-server', '-Xmx8g', F'-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl',f'-Djetty.port={port}', '-jar', f'{ts_dir}/blazegraph.jar']
            )
        else:
            Popen(
                ['java', '-server', '-Xmx8g', F'-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl',f'-Djetty.port={port}', '-jar', f'{ts_dir}/blazegraph.jar'],
                creationflags=CREATE_NEW_CONSOLE
            )

    def launch_graphdb(self, ts_dir:str, port:int=7200):
        '''
        Launch GraphDB triplestore at a given port.
        '''
        graphdb_bin = 'graphdb' if self.is_unix else 'graphdb.cmd'
        abs_path_to_graphdb = os.path.abspath(f'{ts_dir}/bin/{graphdb_bin}')
        if self.is_unix:
            Popen(
                ['gnome-terminal', '--', abs_path_to_graphdb, '-Xmx8g', f'-Dgraphdb.connector.port={port}']
            )
        else:
            Popen(
                [abs_path_to_graphdb, '-Xmx8g', f'-Dgraphdb.connector.port={port}'],
                creationflags=CREATE_NEW_CONSOLE
            )

    def launch_fuseki(self, port:int=7200):
        '''
        Launch Fuseki triplestore at a given port.
        '''
        fuseki_bin = 'fuseki-server' if self.is_unix else 'fuseki-server.bat'
        os.environ['FUSEKI_BASE'] = os.path.abspath('./db/fuseki')
        os.environ['FUSEKI_HOME'] = os.path.abspath('./db/fuseki')
        abs_path_to_fuseki = os.path.abspath(f'./db/fuseki/{fuseki_bin}')
        if self.is_unix:
            Popen(
                ['gnome-terminal', '--', abs_path_to_fuseki, f'--port={str(port)}']
            )
        else:
            Popen(
                [abs_path_to_fuseki, f'--port={str(port)}'],
                creationflags=CREATE_NEW_CONSOLE
            )

    def launch_virtuoso(self, ts_dir:str):
        '''
        Launch Virtuoso triplestore.
        '''
        virtuoso_platform = 'linux' if self.is_unix else 'windows'
        virtuoso_base = f'./db/virtuoso/{virtuoso_platform}'
        virtuoso_database = f'{virtuoso_base}/{ts_dir}'
        virtuoso_bin = 'virtuoso-t' if self.is_unix else 'virtuoso-t.exe'
        if os.path.exists(f'{virtuoso_database}/virtuoso.lck'):
            os.remove(f'{virtuoso_database}/virtuoso.lck')
        if self.is_unix:
            Popen(
                ['gnome-terminal', '--', f'{virtuoso_base}/bin/{virtuoso_bin}', '-f', '-c', f'{virtuoso_database}/virtuoso.ini']
            )
        else:
            Popen(
                [f'{virtuoso_base}/bin/{virtuoso_bin}', '-f', '-c', f'{virtuoso_base}/{ts_dir}/virtuoso.ini'],
                creationflags=CREATE_NEW_CONSOLE
            )

    @staticmethod
    def clear_cache(triplestore):
        config_filepath = f'config/{triplestore.lower()}/w_cache_w_out_index.json'
        _, setup_cache = get_setup(triplestore)
        exec(setup_cache, globals())
        empty_the_cache(config_filepath)