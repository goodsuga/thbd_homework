import os
from pathlib import Path
import zipfile
import pandas as pd
import argparse
from time import time

from archiver import compress, decompress

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="archtest",
        usage=(
            "Test and compare self-written"
            " archiver to existing methods"
        )
    )
    parser.add_argument(
        "--source", "-s", "-S",
        required=True,
        type=Path,
        help="Path to the file to compress"
    )
    parser.add_argument(
        "--destination", "-d", "-D",
        required=True,
        type=Path,
        help="Path to store the compressed file at"
    )
    parser.add_argument(
        "--reverse", "-r", "-R",
        required=True,
        type=Path,
        help=(
            "Path to the file to which the "
            "decompressed data will be written"
        )
    )

    print("Running self-made archiver")
    args = parser.parse_args()

    start = time()
    compress(400, 15, args.source, args.destination)
    end = time()
    compression_time = round(end-start, 4)
    start = time()
    decompress(args.destination, args.reverse)
    end = time()
    decompression_time = round(end-start, 4)

    # Пустим несколько тестов
    fsize_original = os.stat(args.source).st_size
    fsize_compressed = os.stat(args.destination).st_size
    fsize_decompressed = os.stat(args.reverse).st_size

    assert fsize_compressed <= fsize_original, "Compression failed!"
    assert fsize_decompressed == fsize_original, "Decompression failed! (File size mismatch)"

    with open(args.source, "rb") as src:
        data_src = src.read()
        with open(args.reverse, "rb") as rvr:
            data_dcmp = rvr.read()
    FAIL_STR = "Decompression failed! (File content mismatch)"
    check = all([b1 == b2 for b1, b2 in zip(data_src, data_dcmp)])
    assert check, FAIL_STR

    statistics = [[
        "self-made LZ77",
        compression_time,
        decompression_time,
        round(fsize_compressed / fsize_original, 5)
    ]]

    # По той же процедуре, но используя встроенные в питон LZMA, BZIP, DEFLATE
    comp_methods = [
        zipfile.ZIP_LZMA,
        zipfile.ZIP_DEFLATED,
        zipfile.ZIP_BZIP2
    ]
    comp_names = [
        "LZMA",
        "DEFLATED",
        "BZIP2"
    ]
    for comp_method, comp_name in zip(comp_methods, comp_names):
        start = time()
        with zipfile.ZipFile(args.destination, 'w', compression=comp_method) as zip: 
            zip.write(args.source, args.reverse)
        end = time()
        compression_time = round(end-start, 4)

        start = time()
        with zipfile.ZipFile(args.destination, 'r', compression=comp_method) as zip: 
            zip.extractall() 
        end = time()
        decompression_time = round(end-start, 4)

        fsize_original = os.stat(args.source).st_size
        fsize_compressed = os.stat(args.destination).st_size
        
        statistics.append([
            comp_name,
            compression_time,
            decompression_time,
            round(fsize_compressed / fsize_original, 5)
        ])
    
    statistics = pd.DataFrame(
        statistics,
        columns=[
            "Method name",
            "Compression time (s)",
            "Decompression time (s)",
            "Compression ratio"
    ])

    print(statistics)
    statistics.to_csv("stats.csv", index=False)

    
