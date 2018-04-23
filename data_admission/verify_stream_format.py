# Copyright (c) 2017, MD2K Center of Excellence
# - Nasir Ali <nasir.ali08@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
# list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
# this list of conditions and the following disclaimer in the documentation
# and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


import uuid
import argparse
import os, gzip, pickle
from cerebralcortex.cerebralcortex import CerebralCortex

class VerifyStreamFormat():
    def __init__(self, study_name, CC, user_id:uuid=None):
        self.CC = CC
        self.study_name = study_name

        if user_id is None:
            self.all_users_data(study_name)
        else:
            self.one_user_data(user_id)

    def one_user_data(self, user_id: uuid):
        if user_id:
            self.process(user_id)
        else:
            print("User id cannot be empty.")


    def all_users_data(self, study_name: str):
        all_users = self.CC.get_all_users(study_name)

        if all_users:
            for user in all_users:
                self.process(user["identifier"])
        else:
            print(study_name, "- study has 0 users.")


    def process(self, user_id: uuid):

        # get all the streams belong to a participant
        streams = self.CC.get_user_streams(user_id)
        with open("results.txt", "w") as output:
            for stream_name, stream_info in streams.items():
                if stream_info["stream_ids"] is not None and stream_info["stream_ids"] !="":
                    for stream_id in stream_info["stream_ids"]:
                        days = self.CC.get_stream_days(stream_id)
                        for day in days:
                            result = self.read_file(stream_id, user_id, day)
                            output.write(result+"\n")
                            print(result)

    def read_file(self, stream_id, user_id, day):
        data = None
        filename = self.CC.config["data_ingestion"]["filesystem_path"] + str(user_id) + "/" + str(stream_id) + "/" + str(day) + ".gz"
        if os.path.exists(filename):
            try:
                curfile = open(filename, "rb")
                data = curfile.read()
                curfile.close()
            except:
                raise Exception
        try:
            if data is not None and data != b'':
                data = gzip.decompress(data)
                data = pickle.loads(data)
        except:
            raise Exception

        if data is not None and len(data)>0:
            if data[0].offset is None or data[0].offset=="":
                return filename
        # else:
        #     return "Data is empty."



if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex-Script to verify stream-format.')
    parser.add_argument("-conf", "--conf", help="Configuration file path", required=True)
    parser.add_argument("-study_name", "--study_name", help="Configuration file path", required=True)
    parser.add_argument("-uid", "--uid", help="User ID only if verification needs to be performed on a single participant", required=False)
    args = vars(parser.parse_args())

    CC = CerebralCortex(args["conf"])

    VerifyStreamFormat(args["study_name"],CC, args["uid"])
