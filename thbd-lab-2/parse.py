import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.orc as orc
from pathlib import Path
from time import time
import os
import pandas as pd


filename = Path("input.csv")
parquet_output = Path("parquet_out.parquet")
orc_output = Path("orc_output.orc")

stats = []

original_filesize = os.stat(filename).st_size
stats.append([original_filesize / 1024**2, None, None, 100])


print(f"Original filesize = {(original_filesize  / 1024**2):.5f} megabytes")
table = pv.read_csv(filename)
start = time()
pq.write_table(table, parquet_output)
write_time = time() - start
start = time()
pq.read_table(parquet_output)
read_time = time() - start

stats.append([
    round(os.stat(parquet_output).st_size / 1024**2, 5),
    round(write_time, 5),
    round(read_time, 5),
    round(os.stat(parquet_output).st_size * 100 / original_filesize, 5),
])
print(f"Writing a parquet table took {(write_time):5f} seconds")
print(f"Reading it back took {read_time:.5f} seconds")
print(f"Parquet filesize = {(os.stat(parquet_output).st_size / 1024**2):.5f} megabytes")
print(f"In % to original filesize = {(os.stat(parquet_output).st_size * 100 / original_filesize):.5f}")


start = time()
orc.write_table(table, orc_output)
write_time = time() - start
start = time()
orc.read_table(orc_output)
read_time = time() - start

stats.append([
    round(os.stat(orc_output).st_size / 1024**2, 5),
    round(write_time, 5),
    round(read_time, 5),
    round(os.stat(orc_output).st_size * 100 / original_filesize, 5),
])


print(f"Writing an orc table took {(time()-start):5f} seconds")
print(f"Reading it back took {read_time:.5f} seconds")
print(f"Orc filesize = {(os.stat(orc_output).st_size / 1024**2):.5f} megabytes")
print(f"In % to original filesize = {(os.stat(orc_output).st_size * 100 / original_filesize):.5f}")


print("\n-----\nSUMMARY\n")
stats = pd.DataFrame(stats, columns=["Size (MB)", "Write time (seconds)", "Read time (seconds", "% to the original size"])
stats.index = ["Original", "Parquet", "ORC"]
print(stats)
