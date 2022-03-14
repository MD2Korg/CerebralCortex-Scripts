# INSERT INTO user2 (row_id,user_id,username,password,study_name,token,token_issued,token_expiry,user_role,user_metadata,user_settings,active,confirmed_at)
# select row_id,user_id,username,password,JSON_UNQUOTE(JSON_EXTRACT(user_metadata, '$.study_name')) as study_name,token,token_issued,token_expiry,user_role,user_metadata,"{}",active,confirmed_at from user;

# INSERT INTO user (user_id,username,password, study_name, token,token_issued,token_expiry,user_role,user_metadata,user_settings,active,creation_date)
# select identifier,username,password,JSON_UNQUOTE(JSON_EXTRACT(user_metadata, '$.study_name')) as study_name, token,token_issued,token_expiry,user_role,user_metadata,"{}",active,confirmed_at from user_dagobah;

from mysql import connector
import os
import json
import glob
import pandas as pd
from datetime import datetime

db_name = "cerebralcortex"
db_user = "root"
db_password = "pass"
db_url = "localhost"

conn = connector.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor1(dictionary=True)

csv_file_path = "/home/ali/IdeaProjects/MD2K_DATA/mysql_bk/summary.csv"
with open(csv_file_path, "r") as fd:
    csv_line = fd.readlines()
