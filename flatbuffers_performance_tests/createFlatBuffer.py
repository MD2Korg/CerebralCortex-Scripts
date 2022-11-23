import flatbuffers
import random
import datetime
import time

import CCupload.Test.UploadRows
import CCupload.Test.row_object
import CCupload.Test.data
import CCupload.Test.accelData


def generate_sample_data(builder, num_of_samples):
    print("Generating samples...")
    datapoints = [i for i in range(0, num_of_samples)]
    row_objects = [i for i in range(0, num_of_samples)]
    for i in range(0, num_of_samples):
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
        return row_objects


def add_rows_vector(builder, row_objects, num_of_samples):
    print("Starting rows vector...")
    CCupload.Test.UploadRows.UploadRowsStartRowsVector(builder, num_of_samples)
    for i in range(0, num_of_samples):
        # Prepend row_objects to UploadRows
        builder.PrependUOffsetTRelative(row_objects[i])
    upload_rows = builder.EndVector(num_of_samples)
    print("Row vector completed.")
    return upload_rows


def open_flatbuffer():
    builder = flatbuffers.Builder(1024)
    to_upload = builder.CreateString("Upload")
    return builder


def create_flatbuffer(builder, upload_rows):
    print("Creating flatbuffer...")
    # Create flatbuffer
    CCupload.Test.UploadRows.UploadRowsStart(builder)
    CCupload.Test.UploadRows.UploadRowsAddRows(builder, upload_rows)
    upload = CCupload.Test.UploadRows.UploadRowsEnd(builder)
    builder.Finish(upload)
    # This must be called after Finish()
    upload_buffer = builder.Output()
    # upload_buffer must be transferred as binary, not text
    return upload_buffer


def write_to_file(upload_buffer, file_name):
    print("Saving flatbuffer to disk...")
    # Save flatbuffer to disk
    filename = file_name + '.dat'
    with open(filename, 'wb') as f:
        f.write(upload_buffer)
