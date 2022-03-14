# INSERT INTO user2 (row_id,user_id,username,password,study_name,token,token_issued,token_expiry,user_role,user_metadata,user_settings,active,confirmed_at)
# select row_id,user_id,username,password,JSON_UNQUOTE(JSON_EXTRACT(user_metadata, '$.study_name')) as study_name,token,token_issued,token_expiry,user_role,user_metadata,"{}",active,confirmed_at from user;

# INSERT INTO user (user_id,username,password, study_name, token,token_issued,token_expiry,user_role,user_metadata,user_settings,active,creation_date)
# select identifier,username,password,JSON_UNQUOTE(JSON_EXTRACT(user_metadata, '$.study_name')) as study_name, token,token_issued,token_expiry,user_role,user_metadata,"{}",active,confirmed_at from user_dagobah;

import pymysql

db_name = "environmental_data"
db_user = "root"
db_password = "pass"
db_url = "localhost"

conn = pymysql.connect(host=db_url, database=db_name, user=db_user, password=db_password)

cursor = conn.cursor(pymysql.cursors.DictCursor)

def get_weather_data():
    qry = 'SELECT id,location, sunrise, sunset, temperature->"$.temp" as temp, temperature->"$.temp_min" as temp_min, temperature->"$.temp_max" as temp_max,  humidity, wind->"$.deg" as wind_deg, wind->"$.speed" as wind_speed, clouds, snow, detailed_status, added_date as date_time FROM `weather`'
    all_vals = "country,country_code,city,zip,latitude,longitude,sunrise,sunset,temp,temp_min,temp_max,humidity,wind_deg,wind_speed,clouds,snow,detailed_status,date_time"
    cursor.execute(qry)
    rows = cursor.fetchall()
    for row in rows:
        print(row['id'])
        loc_info = get_location_info(row['location'])
        all_vals += loc_info+","+str(row['sunrise'])+","+str(row['sunset'])+","+str(row['temp'])+","+str(row['temp_min'])+","+str(row['temp_max'])+","+str(row['humidity'])+","+str(row['wind_deg'])+","+str(row['wind_speed'])+","+str(row['clouds'])+","+str(row['snow'])+","+str(row['detailed_status'])+","+str(row['date_time'])+"\n"
    return all_vals

def get_location_info(loc_id):
    qry = 'SELECT * from locations where id='+str(loc_id)
    all_vals = []
    cursor.execute(qry)
    for row in cursor:
        return row['country']+","+row['country_code']+","+row['city']+","+row['zip']+","+row['latitude']+","+row['longitude']
    return all_vals

data = get_weather_data()
csv_file_path = "weather_data.csv"
with open(csv_file_path, "w") as fd:
    fd.write(data)

