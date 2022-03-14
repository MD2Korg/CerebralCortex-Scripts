from mysql import connector
import pandas as pd
import json

db_name = "cerebralcortex"
db_user = "root"
db_password = "pass"
db_url = "localhost"
data_folder_path = "/Users/ali/IdeaProjects/MD2K_DATA/WESAD/"

conn = connector.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor(dictionary=True)

def get_user_inf(i):
    file_path = data_folder_path+"S"+str(i)+"/S"+str(i)+"_readme.txt"
    df = pd.read_csv(file_path, sep=':|\?', engine="python")
    return df.to_dict()


for i in range(2,20):
    try:
        user_metadata = get_user_inf(i)
        user = str("S"+str(i)).lower()
        qry = "INSERT INTO user (user_id, username, password, study_name, user_role, user_metadata, user_settings, active, has_data) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
        vals = user, user, "", "wesad", "wesad_participant", json.dumps(user_metadata), "{}",  "1", "1"
        cursor.execute(qry, vals)
        conn.commit()
    except Exception as e:
        print(e)