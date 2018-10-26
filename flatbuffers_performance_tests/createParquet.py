import flatbuffers
import random
import datetime
import time

import pandas
from fastparquet import write

import CCupload.Test.UploadRows
import CCupload.Test.row_object
import CCupload.Test.data
import CCupload.Test.accelData

begin = datetime.datetime.now()
beginmilli = int(time.mktime(begin.timetuple()) + begin.microsecond/1000000)

with open('flatbuffer_file.dat', 'rb') as f:
    fbinput = f.read()

uploadRows = CCupload.Test.UploadRows.UploadRows.GetRootAsUploadRows(fbinput, 0)

data = []
for i in range(0, 1000000):
    row = [uploadRows.Rows(i).RowKey(), uploadRows.Rows(i).Datapoint().Datetime(),
           uploadRows.Rows(i).Datapoint().Sample().X(), uploadRows.Rows(i).Datapoint().Sample().Y(),
           uploadRows.Rows(i).Datapoint().Sample().Z()]
    data.append(row)

df = pandas.DataFrame(data, columns=['RowKey', 'Timestamp', 'X', 'Y', 'Z'])

mid = datetime.datetime.now()
midmilli = int(time.mktime(mid.timetuple()) + mid.microsecond/1000000)
print("DataFrame created in " + str(midmilli - beginmilli) + " milliseconds.")
print("Parquet writing started....")

# Write the dataframe to parquet
write('parquet_file.parq', df)


end = datetime.datetime.now()
endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
print("Parquet file created in " + str(endmilli - midmilli) + " milliseconds.")
print("Total time: " + str(endmilli - beginmilli) + " milliseconds.")