from subprocess import Popen, CREATE_NEW_CONSOLE
import os
import psutil
import signal


class TriplestoreManager:
    @classmethod
    def close_program_by_name(cls, name):
        pids = cls.__find_pid_by_name(name)
        for pid in pids:
            try:
                # Linux
                os.kill(pid, signal.SIGSTOP)
            except AttributeError:
                # Windows
                os.kill(pid, signal.SIGTERM)

    @classmethod
    def __find_pid_by_name(cls, processName):
        listOfProcessObjects = []
        for proc in psutil.process_iter():
            try:
                pinfo = proc.as_dict(attrs=['pid', 'name'])
                # Check if process name contains the given name string.
                if processName.lower() in pinfo['name'].lower() :
                    listOfProcessObjects.append(pinfo['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied , psutil.ZombieProcess):
                pass
        return listOfProcessObjects

    @staticmethod
    def launch_blazegraph(ts_dir:str, port:int):
        '''
        Launch Blazegraph triplestore at a given port.
        '''
        Popen(
            ['java', '-server', '-Xmx4g', F'-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl',f'-Djetty.port={port}', '-jar', f'{ts_dir}/blazegraph.jar'],
            creationflags=CREATE_NEW_CONSOLE
        )

    @staticmethod
    def launch_graphdb(ts_dir:str, port:int=7200):
        '''
        Launch GraphDB triplestore at a given port.
        '''
        abs_path_to_graphdb = os.path.abspath(f'{ts_dir}/bin/graphdb.cmd')
        Popen(
            [abs_path_to_graphdb, '-Xmx16g', f'-Dgraphdb.connector.port={port}'],
            creationflags=CREATE_NEW_CONSOLE
        )