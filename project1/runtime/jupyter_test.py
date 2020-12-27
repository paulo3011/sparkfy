from create_tables import main as createdb
from etl import main as runetl

def runt_etl():
    print("starting etl test")
    createdb()
    runetl(path="/home/paulo/projects/paulo3011/sparkfy/project1/runtime/")


if __name__ == "__main__":
    runt_etl()