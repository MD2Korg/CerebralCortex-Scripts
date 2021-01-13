# read csv file that has all the list of user_ids and stream_names that were marked as processed in mysql but were not available in hdfs

import mysql.connector
import csv

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mEm62$%YsoK*88!",
    database="cerebralcortex"
)
mycursor = mydb.cursor1()

qry = 'update ingestion_logs_md2k_md2k2 set fault_type="", fault_description="", success=5 where user_id="038aafca-cc30-47c6-9cbe-5c2cb52d8f04" and stream_name="cu_notif_rm_tickertext--edu.dartmouth.eureka"'

update_vals = []
with open("missed_streams_users_for_processing.csv", "r") as csvfile:
    readCSV = csv.reader(csvfile, delimiter=',')
    for row in readCSV:
        update_vals.append((row[0],row[1]))

print("STARTED UPDATED MYSQL TABLE")
mycursor.executemany(qry, update_vals)
mydb.commit()

print("COMPLETED UPDATE")