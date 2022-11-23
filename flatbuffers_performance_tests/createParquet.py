import pandas
from fastparquet import write

import CCupload.Test.UploadRows
import CCupload.Test.accelData


def open_file(filename):
    print("Opening flatbuffer...")
    with open(filename, 'rb') as f:
        fbinput = f.read()
    uploadRows = CCupload.Test.UploadRows.UploadRows.GetRootAsUploadRows(fbinput, 0)
    return uploadRows


def create_dataframe(uploadRows):
    print("Creating dataframe...")
    data = []
    for i in range(uploadRows.RowsLength()):
        row = [uploadRows.Rows(i).RowKey(), uploadRows.Rows(i).Datapoint().Datetime(),
               uploadRows.Rows(i).Datapoint().Sample().X(), uploadRows.Rows(i).Datapoint().Sample().Y(),
               uploadRows.Rows(i).Datapoint().Sample().Z()]
        data.append(row)

    df = pandas.DataFrame(data, columns=['RowKey', 'Timestamp', 'X', 'Y', 'Z'])
    return df


def write_parquet(df, file_name):
    print("Parquet writing started....")
    filename = file_name + '.parq'
    # Write the dataframe to parquet
    write(filename, df)
