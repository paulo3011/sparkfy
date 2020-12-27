import os
import glob
import psycopg2
import pandas as pd
from file_functions import *
import petl as etl

print("ok")

file_list = get_files("/usr/src/app/data/")

# print(file_list)

file_song = file_list.pop()

table = etl.fromjson(source=file_song, lines=True)
print(etl.header(table),"\n")
print(etl.head(table,n=2))
#print(table)

df = pd.read_json(path_or_buf=file_song, lines=True)
print(df.head())
values = df.values
song_data = values[0]
print(song_data)