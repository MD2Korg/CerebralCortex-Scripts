"""
Run 360,000 records, appended 24 times
Run 5,000 records, appended 10 times
compress with snappy
compress with gzip
uncompressed
"""
import lessNestedFlatBuffer
import lessNestedParquet
import datetime
import time
import os


def standardTestLessNested5KSamples():
    print("Starting less nested standard test...")
    num_of_samples = 5000
    beginStandardLN = datetime.datetime.now()

    builder = lessNestedFlatBuffer.open_flatbuffer()
    rows = lessNestedFlatBuffer.generate_sample_data(builder, num_of_samples)

    startFB = datetime.datetime.now()
    upload_buffer = lessNestedFlatBuffer.create_flatbuffer(builder, rows)

    startWrite = datetime.datetime.now()
    filename = 'flatbuffer'
    filepath = filename + '.dat'
    lessNestedFlatBuffer.write_to_file(upload_buffer, filename)
    end = datetime.datetime.now()

    beginStandardLNmilli = int(time.mktime(beginStandardLN.timetuple()) + beginStandardLN.microsecond/1000000)
    startFBmilli = int(time.mktime(startFB.timetuple()) + startFB.microsecond/1000000)
    startWritemilli = int(time.mktime(startWrite.timetuple()) + startWrite.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    print("Samples generated in " + str(startFBmilli - beginStandardLNmilli) + " milliseconds.")
    print("Flatbuffer created in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("File written in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginStandardLNmilli) + " milliseconds.")
    print("Flatbuffer file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("\n")

    print("Starting standard Parquet conversion...")
    beginStandardP = datetime.datetime.now()
    rows = lessNestedParquet.open_file(filepath)
    beginDF = datetime.datetime.now()
    dataframe = lessNestedParquet.create_dataframe(rows)
    beginParquetWrite = datetime.datetime.now()
    filename = 'parquet'
    filepath = filename + '.parq'
    lessNestedParquet.write_parquet(dataframe, filename)

    print("Starting Snappy compressed Parquet conversion...")
    beginSnappyParquetWrite = datetime.datetime.now()
    snapfilename = 'snappyParquet'
    snapfilepath = snapfilename + '.parq'
    lessNestedParquet.write_parquet_snappy(dataframe, snapfilename, num_of_samples)
    endSnappy = datetime.datetime.now()

    print("Starting Gzip compressed Parquest conversion...")
    beginGzipParquetWrite = datetime.datetime.now()
    gzipfilename = 'gzipParquet'
    gzipfilepath = gzipfilename + '.parq'
    lessNestedParquet.write_parquet_gzip(dataframe, gzipfilename, num_of_samples)
    endGzip = datetime.datetime.now()

    end = datetime.datetime.now()
    beginStandardPmilli = int(time.mktime(beginStandardP.timetuple()) + beginStandardP.microsecond/1000000)
    beginDFmilli = int(time.mktime(beginDF.timetuple()) + beginDF.microsecond/1000000)
    beginParquetWritemilli = int(time.mktime(beginParquetWrite.timetuple()) + beginParquetWrite.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    beginSnappyParquetWritemilli = int(time.mktime(beginSnappyParquetWrite.timetuple()) + beginSnappyParquetWrite.microsecond/1000000)
    beginGzipParquetWritemilli = int(time.mktime(beginGzipParquetWrite.timetuple()) + beginGzipParquetWrite.microsecond/1000000)
    endSnappymilli = int(time.mktime(endSnappy.timetuple()) + endSnappy.microsecond/1000000)
    endGzipmilli = int(time.mktime(endGzip.timetuple()) + endGzip.microsecond/1000000)
    print("Flatbuffer read in " + str(beginDFmilli - beginStandardPmilli) + " milliseconds.")
    print("Dataframe created in " + str(beginParquetWritemilli - beginDFmilli) + " milliseconds.")
    print("Parquet written to disk in " + str(beginSnappyParquetWritemilli - beginParquetWritemilli) + " milliseconds.")
    print("Snappy compressed Parquet written to disk in " + str(endSnappymilli - beginSnappyParquetWritemilli) + " milliseconds.")
    print("Gzip compressed Parquet written to disk in " + str(endGzipmilli - beginGzipParquetWritemilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginStandardPmilli) + " milliseconds.")
    print("Parquet file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("Snappy compressed Parquet file size is " + str(os.path.getsize(snapfilepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("Gzip compressed Parquet file size is " + str(os.path.getsize(gzipfilepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("\n\n")


def appendTestLessNested5KSamples():
    print("Starting less nested append test...")
    num_of_samples = 5000
    beginAppendLN = datetime.datetime.now()

    n = 10
    for i in range(n):
        builder = lessNestedFlatBuffer.open_flatbuffer()
        rows = lessNestedFlatBuffer.generate_sample_data(builder, num_of_samples)

        startFB = datetime.datetime.now()
        upload_buffer = lessNestedFlatBuffer.create_flatbuffer(builder, rows)

        startWrite = datetime.datetime.now()
        filename = 'flatbuffer' + str(i)
        lessNestedFlatBuffer.write_to_file(upload_buffer, filename)
    filepaths = ['flatbuffer' + str(i) + '.dat' for i in range(n)]
    end = datetime.datetime.now()

    beginAppendLNmilli = int(time.mktime(beginAppendLN.timetuple()) + beginAppendLN.microsecond/1000000)
    startFBmilli = int(time.mktime(startFB.timetuple()) + startFB.microsecond/1000000)
    startWritemilli = int(time.mktime(startWrite.timetuple()) + startWrite.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    print("Samples generated in " + str(startFBmilli - beginAppendLNmilli) + " milliseconds.")
    print("Flatbuffer created in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("File written in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginAppendLNmilli) + " milliseconds.")
    print("One flatbuffer file size is " + str(os.path.getsize(filepaths[0])) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("\n")

    print("Starting appended Parquet conversion...")
    beginAppendP = datetime.datetime.now()
    rowgroups = [0] * n
    dataframes = [0] * n
    for i in range(len(filepaths)):
        print("Reading rowgroup " + str(i) + "...")
        rowgroups[i] = lessNestedParquet.open_file(filepaths[i])
        dataframes[i] = lessNestedParquet.create_dataframe(rowgroups[i])
        filename = 'append5KParquet'
        filepath = filename + '.parq'
        if i == 0:
            print("Writing initial parquet file...")
            lessNestedParquet.write_parquet(dataframes[i], filename)
        else:
            print("Appending rowgroup " + str(i) + "...")
            lessNestedParquet.write_parquet_append(dataframes[i], filename)

    print("Appending Snappy compressed Parquet conversion...")
    snapfilename = 'snappyAppend5KParquet'
    snapfilepath = snapfilename + '.parq'
    for i in range(len(filepaths)):
        if i == 0:
            lessNestedParquet.write_parquet_snappy(dataframes[i], snapfilename, num_of_samples)
        else:
            lessNestedParquet.write_parquet_snappy(dataframes[i], snapfilename, num_of_samples)

    print("Appending Gzip compressed Parquest conversion...")
    gzipfilename = 'gzipAppend5KParquet'
    gzipfilepath = gzipfilename + '.parq'
    for i in range(len(filepaths)):
        if i == 0:
            lessNestedParquet.write_parquet_gzip(dataframes[i], gzipfilename, num_of_samples)
        else:
            lessNestedParquet.write_parquet_gzip(dataframes[i], gzipfilename, num_of_samples)

    end = datetime.datetime.now()
    beginAppendPmilli = int(time.mktime(beginAppendP.timetuple()) + beginAppendP.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    print("Total time " + str(endmilli - beginAppendPmilli) + " milliseconds.")
    print("Parquet file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(num_of_samples * n) + " rows of data.")
    print("Snappy compressed Parquet file size is " + str(os.path.getsize(snapfilepath)) + " bytes for " + str(num_of_samples * n) + " rows of data.")
    print("Gzip compressed Parquet file size is " + str(os.path.getsize(gzipfilepath)) + " bytes for " + str(num_of_samples * n) + " rows of data.")
    print("\n\n")


def standardTestLessNested360KSamples():
    print("Starting less nested standard test...")
    num_of_samples = 360000
    beginStandardLN = datetime.datetime.now()

    builder = lessNestedFlatBuffer.open_flatbuffer()
    rows = lessNestedFlatBuffer.generate_sample_data(builder, num_of_samples)

    startFB = datetime.datetime.now()
    upload_buffer = lessNestedFlatBuffer.create_flatbuffer(builder, rows)

    startWrite = datetime.datetime.now()
    filename = 'flatbuffer'
    filepath = filename + '.dat'
    lessNestedFlatBuffer.write_to_file(upload_buffer, filename)
    end = datetime.datetime.now()

    beginStandardLNmilli = int(time.mktime(beginStandardLN.timetuple()) + beginStandardLN.microsecond/1000000)
    startFBmilli = int(time.mktime(startFB.timetuple()) + startFB.microsecond/1000000)
    startWritemilli = int(time.mktime(startWrite.timetuple()) + startWrite.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    print("Samples generated in " + str(startFBmilli - beginStandardLNmilli) + " milliseconds.")
    print("Flatbuffer created in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("File written in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginStandardLNmilli) + " milliseconds.")
    print("Flatbuffer file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("\n")

    print("Starting standard Parquet conversion...")
    beginStandardP = datetime.datetime.now()
    rows = lessNestedParquet.open_file(filepath)
    beginDF = datetime.datetime.now()
    dataframe = lessNestedParquet.create_dataframe(rows)
    beginParquetWrite = datetime.datetime.now()
    filename = 'parquet'
    filepath = filename + '.parq'
    lessNestedParquet.write_parquet(dataframe, filename)


    print("Starting Snappy compressed Parquet conversion...")
    beginSnappyParquetWrite = datetime.datetime.now()
    snapfilename = 'snappyParquet'
    snapfilepath = snapfilename + '.parq'
    lessNestedParquet.write_parquet_snappy(dataframe, snapfilename, num_of_samples)
    endSnappy = datetime.datetime.now()

    print("Starting Gzip compressed Parquest conversion...")
    beginGzipParquetWrite = datetime.datetime.now()
    gzipfilename = 'gzipParquet'
    gzipfilepath = gzipfilename + '.parq'
    lessNestedParquet.write_parquet_gzip(dataframe, gzipfilename, num_of_samples)
    endGzip = datetime.datetime.now()

    end = datetime.datetime.now()
    beginStandardPmilli = int(time.mktime(beginStandardP.timetuple()) + beginStandardP.microsecond/1000000)
    beginDFmilli = int(time.mktime(beginDF.timetuple()) + beginDF.microsecond/1000000)
    beginParquetWritemilli = int(time.mktime(beginParquetWrite.timetuple()) + beginParquetWrite.microsecond/1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond/1000000)
    beginSnappyParquetWritemilli = int(time.mktime(beginSnappyParquetWrite.timetuple()) + beginSnappyParquetWrite.microsecond/1000000)
    beginGzipParquetWritemilli = int(time.mktime(beginGzipParquetWrite.timetuple()) + beginGzipParquetWrite.microsecond/1000000)
    endSnappymilli = int(time.mktime(endSnappy.timetuple()) + endSnappy.microsecond/1000000)
    endGzipmilli = int(time.mktime(endGzip.timetuple()) + endGzip.microsecond/1000000)
    print("Flatbuffer read in " + str(beginDFmilli - beginStandardPmilli) + " milliseconds.")
    print("Dataframe created in " + str(beginParquetWritemilli - beginDFmilli) + " milliseconds.")
    print("Parquet written to disk in " + str(beginSnappyParquetWritemilli - beginParquetWritemilli) + " milliseconds.")
    print("Snappy compressed Parquet written to disk in " + str(endSnappymilli - beginSnappyParquetWritemilli) + " milliseconds.")
    print("Gzip compressed Parquet written to disk in " + str(endGzipmilli - beginGzipParquetWritemilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginStandardPmilli) + " milliseconds.")
    print("Parquet file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("Snappy compressed Parquet file size is " + str(os.path.getsize(snapfilepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("Gzip compressed Parquet file size is " + str(os.path.getsize(gzipfilepath)) + " bytes for " + str(num_of_samples) + " rows of data.")
    print("\n\n")


def appendTestLessNested360KSamples():
    print("Starting less nested append test...")
    num_of_samples = 360000
    beginAppendLN = datetime.datetime.now()

    n = 24
    for i in range(n):
        builder = lessNestedFlatBuffer.open_flatbuffer()
        rows = lessNestedFlatBuffer.generate_sample_data(builder, num_of_samples)

        startFB = datetime.datetime.now()
        upload_buffer = lessNestedFlatBuffer.create_flatbuffer(builder, rows)

        startWrite = datetime.datetime.now()
        filename = 'flatbuffer' + str(i)
        lessNestedFlatBuffer.write_to_file(upload_buffer, filename)
    filepaths = ['flatbuffer' + str(i) + '.dat' for i in range(n)]
    end = datetime.datetime.now()

    beginAppendLNmilli = int(time.mktime(beginAppendLN.timetuple()) + beginAppendLN.microsecond / 1000000)
    startFBmilli = int(time.mktime(startFB.timetuple()) + startFB.microsecond / 1000000)
    startWritemilli = int(time.mktime(startWrite.timetuple()) + startWrite.microsecond / 1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond / 1000000)
    print("Samples generated in " + str(startFBmilli - beginAppendLNmilli) + " milliseconds.")
    print("Flatbuffer created in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("File written in " + str(startWritemilli - startFBmilli) + " milliseconds.")
    print("Total time " + str(endmilli - beginAppendLNmilli) + " milliseconds.")
    print("One flatbuffer file size is " + str(os.path.getsize(filepaths[0])) + " bytes for " + str(
        num_of_samples) + " rows of data.")
    print("\n")
    345, 624, 770
    3, 07
    9, 408
    print("Starting appended Parquet conversion...")
    beginAppendP = datetime.datetime.now()
    rowgroups = [0] * n
    dataframes = [0] * n
    for i in range(len(filepaths)):
        print("Reading rowgroup " + str(i) + "...")
        rowgroups[i] = lessNestedParquet.open_file(filepaths[i])
        dataframes[i] = lessNestedParquet.create_dataframe(rowgroups[i])
        filename = 'append360KParquet'
        filepath = filename + '.parq'
        if i == 0:
            print("Writing initial parquet file...")
            lessNestedParquet.write_parquet(dataframes[i], filename)
        else:
            print("Appending rowgroup " + str(i) + "...")
            lessNestedParquet.write_parquet_append(dataframes[i], filename)

    print("Appending Snappy compressed Parquet conversion...")
    snapfilename = 'snappyAppend360KParquet'
    snapfilepath = snapfilename + '.parq'
    for i in range(len(filepaths)):
        if i == 0:
            lessNestedParquet.write_parquet_snappy(dataframes[i], snapfilename, num_of_samples)
        else:
            lessNestedParquet.write_parquet_snappy(dataframes[i], snapfilename, num_of_samples)

    print("Appending Gzip compressed Parquest conversion...")
    gzipfilename = 'gzipAppend360KParquet'
    gzipfilepath = gzipfilename + '.parq'
    for i in range(len(filepaths)):
        if i == 0:
            lessNestedParquet.write_parquet_gzip(dataframes[i], gzipfilename, num_of_samples)
        else:
            lessNestedParquet.write_parquet_gzip(dataframes[i], gzipfilename, num_of_samples)

    end = datetime.datetime.now()
    beginAppendPmilli = int(time.mktime(beginAppendP.timetuple()) + beginAppendP.microsecond / 1000000)
    endmilli = int(time.mktime(end.timetuple()) + end.microsecond / 1000000)
    print("Total time " + str(endmilli - beginAppendPmilli) + " milliseconds.")
    print("Parquet file size is " + str(os.path.getsize(filepath)) + " bytes for " + str(
        num_of_samples * n) + " rows of data.")
    print("Snappy compressed Parquet file size is " + str(os.path.getsize(snapfilepath)) + " bytes for " + str(
        num_of_samples * n) + " rows of data.")
    print("Gzip compressed Parquet file size is " + str(os.path.getsize(gzipfilepath)) + " bytes for " + str(
        num_of_samples * n) + " rows of data.")
    print("\n\n")


standardTestLessNested5KSamples()
standardTestLessNested360KSamples()
appendTestLessNested5KSamples()
appendTestLessNested360KSamples()
