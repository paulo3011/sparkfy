"""Run a test like when run on udacity environment."""
from create_tables import main as createdb
from etl import main as runetl

def _runt_etl():
    print("starting etl test")
    createdb()
    runetl(path="./")


if __name__ == "__main__":
    _runt_etl()