import flatbuffers
import random
import datetime
import time
import CCupload.Test.uploadRows
import CCupload.Test.rowObject


def open_flatbuffer():
    builder = flatbuffers.Builder(1024)
    # Start building the UploadRows table
    to_upload = builder.CreateString("Upload 1")
    return builder


def generate_sample_data(builder, num_of_samples):
    print("Generating samples...")
    CCupload.Test.uploadRows.uploadRowsStartRowsVector(builder, num_of_samples)
    for i in range(0, num_of_samples):
        # Get time in millisecond timestamp format
        d = datetime.datetime.now()
        now = int(time.mktime(d.timetuple()) + d.microsecond/1000000)

        # Randomly generate accel values
        x = random.uniform(-50.0, 50.0)
        y = random.uniform(-50.0, 50.0)
        z = random.uniform(-50.0, 50.0)

        builder.PrependUOffsetTRelative(CCupload.Test.rowObject.CreaterowObject(builder, i, now, x, y, z))
    upload_rows = builder.EndVector(num_of_samples)
    return upload_rows


def create_flatbuffer(builder, upload_rows):
    print("Creating flatbuffer...")
    # Create flatbuffer
    CCupload.Test.uploadRows.uploadRowsStart(builder)
    CCupload.Test.uploadRows.uploadRowsAddRows(builder, upload_rows)
    upload = CCupload.Test.uploadRows.uploadRowsEnd(builder)
    builder.Finish(upload)

    # This must be called after Finish()
    upload_buffer = builder.Output()
    # upload_buffer must be transferred as binary, not text
    return upload_buffer


def write_to_file(upload_buffer, file_name):
    print("Saving flatbuffer to disk...")
    filename = file_name + '.dat'
    # Save flatbuffer to disk
    with open(filename, 'wb') as f:
        f.write(upload_buffer)
