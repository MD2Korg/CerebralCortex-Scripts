import flatbuffers
import random
import datetime
import time

import CCupload.Test.UploadRows
import CCupload.Test.row_object
import CCupload.Test.data
import CCupload.Test.accelData

begin = datetime.datetime.now()
beginmilli = int(time.mktime(begin.timetuple()) + begin.microsecond/1000000)

builder = flatbuffers.Builder(1024)

# Start building the UploadRows table
to_upload = builder.CreateString("Upload 1")

datapoints = [i for i in range(0, 1000000)]
row_objects = [i for i in range(0, 1000000)]
for i in range(0, 1000000):
    # Get time in millisecond timestamp format
    d = datetime.datetime.now()
    now = int(time.mktime(d.timetuple()) + d.microsecond/1000000)

    # Randomly generate accel values
    x = random.uniform(-50.0, 50.0)
    y = random.uniform(-50.0, 50.0)
    z = random.uniform(-50.0, 50.0)

    # Create the data
    CCupload.Test.data.dataStart(builder)
    CCupload.Test.data.dataAddDatetime(builder, now)
    CCupload.Test.data.dataAddSample(builder, CCupload.Test.accelData.CreateaccelData(builder, x, y, z))
    datapoints[i] = CCupload.Test.data.dataEnd(builder)

    # Create the row_object
    CCupload.Test.row_object.row_objectStart(builder)
    CCupload.Test.row_object.row_objectAddRowKey(builder, i)
    CCupload.Test.row_object.row_objectAddDatapoint(builder, datapoints[i])
    row_objects[i] = CCupload.Test.row_object.row_objectEnd(builder)


CCupload.Test.UploadRows.UploadRowsStartRowsVector(builder, 1000000)
for i in range(0, 1000000):
    # Prepend row_objects to UploadRows
    builder.PrependUOffsetTRelative(row_objects[i])
upload_rows = builder.EndVector(1000000)

# Create flatbuffer
CCupload.Test.UploadRows.UploadRowsStart(builder)
CCupload.Test.UploadRows.UploadRowsAddRows(builder, upload_rows)
upload = CCupload.Test.UploadRows.UploadRowsEnd(builder)
builder.Finish(upload)
# This must be called after Finish()
upload_buffer = builder.Output()
# upload_buffer must be transferred as binary, not text

# Save flatbuffer to disk
with open('flatbuffer_file.dat', 'wb') as f:
    f.write(upload_buffer)

end = datetime.datetime.now()
endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
print("Flatbuffer created in " + str(endmilli - beginmilli) + " milliseconds.")
