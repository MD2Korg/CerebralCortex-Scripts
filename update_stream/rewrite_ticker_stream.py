import mysql.connector
import yaml
import gzip
import os


class UpdateStream():
    """
    Use this only if you know what you are doing! This file can overwrite sample data with "1".
    """
    def __init__(self, config):
        self.config = config
        self.hostIP = config['mysql']['host']
        self.hostPort = config['mysql']['port']
        self.database = config['mysql']['database']
        self.dbUser = config['mysql']['db_user']
        self.dbPassword = config['mysql']['db_pass']
        self.datastreamTable = config['mysql']['datastream_table']

        self.dbConnection = mysql.connector.connect(host=self.hostIP, port=self.hostPort, user=self.dbUser,
                                                    password=self.dbPassword, database=self.database)
        self.cursor = self.dbConnection.cursor(dictionary=True)

    def get_participant_ids(self, stream_name):
        qry = "select identifier, owner, name, start_time, end_time from " + self.datastreamTable + " where name like %(name)s"
        vals = {'name': str("%"+stream_name+"%")}

        self.cursor.execute(qry, vals)
        rows = self.cursor.fetchall()

        if len(rows) > 0:
            for row in rows:
                self.list_stream_files(row["owner"], row["identifier"])

    def list_stream_files(self, owner_id, stream_id):
        data_dir = self.config["data"]["dir"]+owner_id
        if os.path.isdir(data_dir):
            days = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
            if len(days)>0:
                for day in days:
                    stream_dir_path = data_dir+"/"+day+"/"+stream_id+"/"
                    if os.path.isdir(stream_dir_path):
                        stream_files = [d for d in os.listdir(stream_dir_path) if os.path.splitext(os.path.join(stream_dir_path, d))[1]==".gz"]
                        for stream_file in stream_files:
                            self.update_stream_file_contents(stream_dir_path+"/"+stream_file)
                            print("Processed file: ", stream_dir_path, stream_file)


    def update_stream_file_contents(self, filepath: str) -> str:
        fp = gzip.open(filepath)
        new_file_contents = ""
        gzip_file_content = fp.read()
        fp.close()
        gzip_file_content = gzip_file_content.decode('utf-8')
        lines = gzip_file_content.splitlines()
        for line in lines:
            try:
                ts, offset, sample = line.split(',', 2)
                bad_row = 0 # if line is not properly formatted then rest of the code shall not be executed
                try:
                    ts = float(ts)
                except:
                    bad_row = 1
            except:
                bad_row = 1

            if bad_row==0:
                new_file_contents += str(ts)+","+str(offset)+",1\n"

        if new_file_contents!="":
            with gzip.open(filepath, 'wb') as new_file:
                new_file.write(new_file_contents.encode('utf-8'))
                new_file.close()
                new_file_contents=""

    def __del__(self):
        if self.dbConnection:
            self.dbConnection.close()

if __name__ == "__main__":
    with open("config.yml", 'r') as ymlfile:
        config = yaml.load(ymlfile)
        ymlfile.close()

    updateStream = UpdateStream(config)
    updateStream.get_participant_ids("TICKERTEXT")