import zipfile, os.path, time
from subprocess import CREATE_NEW_CONSOLE, Popen


def unzip(file:str, destination:str):
    """
    Unzip the file to the destination directory.
    """
    with zipfile.ZipFile(file, "r") as zip_ref:
        zip_ref.extractall(destination)

def launch_blazegraph(ts_dir:str, port:int):
    """
    Launch Blazegraph triplestore at a given port.
    """
    Popen(
        ["java", "-server", "-Xmx4g", F"-Dcom.bigdata.journal.AbstractJournal.file={ts_dir}/blazegraph.jnl",f"-Djetty.port={port}", "-jar", f"{ts_dir}/blazegraph.jar"],
        creationflags=CREATE_NEW_CONSOLE
    )

def main():
    if not os.path.isfile("tests/blazegraph.jnl"):
        unzip("tests/triplestore.zip", "tests/")
    if not os.path.isfile("tests/cache/blazegraph.jnl"):
        unzip("tests/cache/cache.zip", "tests/cache/")
    launch_blazegraph("tests", 9999)
    launch_blazegraph("tests/cache", 19999)
    time.sleep(10)
    Popen(
        ["python", "-m", "unittest", "discover", "-s", "tests", "-p", "test*.py", "-b"]
    )