import zipfile, os.path, multiprocessing, time
from subprocess import run, CREATE_NEW_CONSOLE, PIPE, STDOUT, Popen


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
        ["java", "-server", "-Xmx4g", f"-Djetty.port={port}", "-jar", f"{ts_dir}/blazegraph.jar"],
        creationflags=CREATE_NEW_CONSOLE
    )

def test():
    """
    Run all unittests.
    """
    Popen(
        ["python", "-m", "unittest", "discover", "-s", "tests", "-p", "test*.py", "-b"]
    )

if not os.path.isfile("tests/blazegraph.jnl"):
    unzip("tests/triplestore.zip", "tests/")
if not os.path.isfile("tests/cache/blazegraph.jnl"):
    unzip("tests/cache/cache.zip", "tests/cache/")
Popen(["cd", "tests"])
Popen(
    ["java", "-server", "-Xmx4g", f"-Djetty.port=9999", "-jar", f"blazegraph.jar"], 
    shell=True
)
Popen(["cd", "tests/cache/"])
Popen(
    ["java", "-server", "-Xmx4g", f"-Djetty.port=19999", "-jar", f"cache/blazegraph.jar"],
    shell=True
)
time.sleep(10)
Popen(["cd", "../../"])
test()
