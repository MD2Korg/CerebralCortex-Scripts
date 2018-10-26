import flatbuffers
import random
import datetime
import time

import CCupload.Test.uploadRows
import CCupload.Test.rowObject

num_of_records = 10000

begin = datetime.datetime.now()
beginmilli = int(time.mktime(begin.timetuple()) + begin.microsecond/1000000)

builder = flatbuffers.Builder(1024)

# Start building the UploadRows table
to_upload = builder.CreateString("Upload 1")

CCupload.Test.uploadRows.uploadRowsStartRowsVector(builder, num_of_records)
for i in range(0, num_of_records):
    # Get time in millisecond timestamp format
    d = datetime.datetime.now()
    now = int(time.mktime(d.timetuple()) + d.microsecond/1000000)

    # Randomly generate accel values
    x = random.uniform(-50.0, 50.0)
    y = random.uniform(-50.0, 50.0)
    z = random.uniform(-50.0, 50.0)

    builder.PrependUOffsetTRelative(CCupload.Test.rowObject.CreaterowObject(builder, i, now, x, y, z))
upload_rows = builder.EndVector(num_of_records)

# Create flatbuffer
CCupload.Test.uploadRows.uploadRowsStart(builder)
CCupload.Test.uploadRows.uploadRowsAddRows(builder, upload_rows)
upload = CCupload.Test.uploadRows.uploadRowsEnd(builder)
builder.Finish(upload)

# This must be called after Finish()
upload_buffer = builder.Output()
# upload_buffer must be transferred as binary, not text

# Save flatbuffer to disk
with open('lessNestedflatbuffer_file.dat', 'wb') as f:
    f.write(upload_buffer)

end = datetime.datetime.now()
endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
print("Flatbuffer created in " + str(endmilli - beginmilli) + " milliseconds.")
