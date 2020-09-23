# some of the users' streams are marked as success in ingestion log table but they do not exist in HDFS
# Find how many cases are there like this one

import mysql.connector
import csv

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="mEm62$%YsoK*88!",
    database="cerebralcortex"
)
mycursor = mydb.cursor()

mycursor.execute("SELECT row_id,user_id,stream_name,file_path,fault_type,fault_description,success,added_date FROM ingestion_logs_md2k_md2k2 group by user_id, stream_name")
rows = mycursor.fetchall()

with open("ingestion_logs_user_stream.csv", "w") as f:
    csv_out=csv.writer(f)
    csv_out.writerow(["row_id","user_id","stream_name","file_path","fault_type","fault_description","success","added_date"])
    for row in rows:
        csv_out.writerow(row)
        # line = row[0]+","+row[1]+","+row[2]+","+row[3]+","+row[4]+","+row[5]+","+row[6]+","+row[7]+"\n"
        # f.write(line)
