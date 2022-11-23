import pandas
from fastparquet import write

import CCupload.Test.uploadRows
import CCupload.Test.rowObject


def open_file(filename):
    print("Opening flatbuffer...")
    with open(filename, 'rb') as f:
        fbinput = f.read()
    upload_rows = CCupload.Test.uploadRows.uploadRows.GetRootAsuploadRows(fbinput, 0)
    return upload_rows


def create_dataframe(upload_rows):
    print("Creating dataframe...")
    data = []
    for i in range(upload_rows.RowsLength()):
        row = [upload_rows.Rows(i).RowKey(), upload_rows.Rows(i).Datetime(), upload_rows.Rows(i).SampleX(),
               upload_rows.Rows(i).SampleY(), upload_rows.Rows(i).SampleZ()]
        data.append(row)

    df = pandas.DataFrame(data, columns=['RowKey', 'Timestamp', 'X', 'Y', 'Z'])
    return df


def write_parquet(df, file_name):
    print("Parquet writing started....")
    filename = file_name + '.parq'
    # Write the dataframe to parquet
    write(filename, df)


def write_parquet_append(df, file_name):
    print("Parquet appending started...")
    filename = file_name + '.parq'
    write(filename, df, append=True)


def write_parquet_snappy(df, file_name, num_of_samples):
    print("Snappy Parquet writing started...")
    filename = file_name + '.parq'
    write(filename, df, num_of_samples, "SNAPPY")


def write_parquet_gzip(df, file_name, num_of_samples):
    print("Gzip Parquet writing started...")
    filename = file_name + '.parq'
    write(filename, df, num_of_samples, "GZIP")
