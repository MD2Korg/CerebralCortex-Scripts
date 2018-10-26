import datetime
import time

import pandas
from fastparquet import write

import CCupload.Test.uploadRows
import CCupload.Test.rowObject

num_of_records = 10000

begin = datetime.datetime.now()
beginmilli = int(time.mktime(begin.timetuple()) + begin.microsecond / 1000000)

with open('lessNestedflatbuffer_file.dat', 'rb') as f:
    fbinput = f.read()

uploadRows = CCupload.Test.uploadRows.uploadRows.GetRootAsuploadRows(fbinput, 0)

data = []
for i in range(0, num_of_records):
    row = [uploadRows.Rows(i).RowKey(), uploadRows.Rows(i).Datetime(), uploadRows.Rows(i).SampleX(),
           uploadRows.Rows(i).SampleY(), uploadRows.Rows(i).SampleZ()]
    data.append(row)

df = pandas.DataFrame(data, columns=['RowKey', 'Timestamp', 'X', 'Y', 'Z'])

mid = datetime.datetime.now()
midmilli = int(time.mktime(mid.timetuple()) + mid.microsecond / 1000000)
print("DataFrame created in " + str(midmilli - beginmilli) + " milliseconds.")
print("Parquet writing started....")

# Write the dataframe to parquet
write('lessNestedParquet_file.parq', df)


end = datetime.datetime.now()
endmilli = int(time.mktime(end.timetuple()) + end.microsecond / 1000000)
print("Parquet file created in " + str(endmilli - midmilli) + " milliseconds.")
print("Total time: " + str(endmilli - beginmilli) + " milliseconds.")