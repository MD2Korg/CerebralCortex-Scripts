# Copyright (c) 2017, MD2K Center of Excellence
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


import os
import json
import yaml
import argparse
from db_helper_methods import SqlData

class ReplayCerebralCortexData:
    def __init__(self, users, config):
        """
        Constructor
        :param configuration:
        """
        self.config = config
        self.data_dir = self.config["data_replay"]["data_dir"]
        if (self.data_dir[-1] != '/'):
            self.data_dir += '/'

        self.sqlData = SqlData(config)
        self.users = users
        self.lab_participants = ['04585052-431f-46cf-bfbc-94bf42c380c8',
                            '04a32e7a-df20-4ed9-a862-553b101dbd95',
                            '04d881cf-7fa7-441b-a6de-e5be72770ca0',
                            '05cb11e7-2c05-4f16-99da-f408a3cc19e9',
                            '09e87864-9585-42d7-ac1f-ff88cce577b0',
                            '0db4b9ac-8bba-45ae-824d-8892695bed08',
                            '12560bbf-8e86-42fa-8b7c-a06a9c987b90',
                            '1bc5b120-7356-4ccb-91c9-d54e189ea905',
                            '1bf149a7-852b-4435-9df8-8d25a8070c73',
                            '1f8c0264-c1b2-46ac-a6f6-d5cb236a3213',
                            '206e63d6-061f-425d-bd28-b845c6dcc8cf',
                            '286078ea-79e3-4a76-a08b-79d9f8f657f2',
                            '28777465-663f-447a-b6e6-015b60d1543a',
                            '353241ef-3289-4510-a193-c6b19ed7f26b',
                            '3941554d-1002-49bf-8b0f-835a52366dee',
                            '39e57530-8c8a-41f6-8659-da7091c1a8eb',
                            '3c27caea-9480-42fd-a4b6-09c0ce940dbb',
                            '3ed9da7f-1df0-4214-8d6a-de9332f8bbcb',
                            '3ff9e162-a249-457d-9582-13a7562db83e',
                            '4a677b36-c14a-4b2d-9dbd-9a9cc22e4686',
                            '4c4271e7-464a-43c8-b976-3e6a705e2b51',
                            '4ec7e880-c7a9-4648-bdc2-32606fbc5bb0',
                            '51bb2efe-d70c-43b5-95e2-9215a54af190',
                            '59a5080b-4230-4b35-bce6-3d9932777903',
                            '59eb1e19-1cf3-4b02-b16a-f2e7bf718e59',
                            '5cf8676c-4be4-432a-afbf-444916b26c99',
                            '602cbc27-a808-4314-ac3e-1d12131e8abd',
                            '629887c7-614f-439d-8485-23455b9b7528',
                            '64ce1bb3-97c8-419b-b1d9-e159cb50a8fa',
                            '6c783bec-673d-4134-a474-19b547b8a88d',
                            '6dc6c0bb-cda0-498e-ba4e-f2e0df866abf',
                            '6f0b8a9b-6fe2-4cda-9825-d0b130f438a4',
                            '6f2bd39a-dcc1-4b8e-a5b8-722a93884e8d',
                            '714b9f6d-8a4b-47fd-ba3a-d277cbec666b',
                            '79ae6619-899e-4c61-8ea7-91611e2db1fe',
                            '7a4fb765-09c4-4ef5-b085-0cb542d29e37',
                            '7bd7979c-1c34-4213-b774-997dfd42c394',
                            '7cf3871c-a7ae-438d-83f8-3249264ff6c1',
                            '82472824-3159-4a7b-b5ff-f4d8dce9b6d9',
                            '8cfa8490-aec2-4032-9452-de9c4dccb94a',
                            '9941d2ee-5222-4e7e-b384-9e022b71f72e',
                            '9b22305d-5327-40a7-8baf-4f54af4cc373',
                            '9d292efa-41e6-4cb3-9778-dd2d3647a72d',
                            'a0d77195-f911-48b8-a1a2-fbf3ed7e668f',
                            'a1e2f165-d872-4c94-9e3f-0d0d6c45fd93',
                            'a236102f-0eb3-49c2-9f80-b28f590e1862',
                            'a26c79be-154c-4e8c-b533-0347e39ea761',
                            'a3ce4c46-01c7-41a8-9591-1686a12673ad',
                            'a6260465-03bc-418f-af1f-b266cb7f1de1',
                            'a762d57b-55ec-42d5-88ec-846d70d780f7',
                            'a9983b52-dec0-4056-93c5-34a5c17476db',
                            'aaa664e1-ba55-4860-9d22-cdbcfc7ffb70',
                            'ac787ead-563f-4d0a-92d1-1c595150dc5f',
                            'b105c482-2d09-48b8-9202-d8e4d97b515f',
                            'b3d3a191-b828-42f9-9f19-958c76a8e639',
                            'b8daecd6-1276-4b6b-9ed6-0ad8d2afad26',
                            'bb385c88-2ccb-4360-ba45-6700d0961749',
                            'c2bb85e4-968f-40ff-845d-01a4751dd8d9',
                            'c8c42ae1-4fda-48c8-8d19-58c7b76d1c3e',
                            'd682722e-51ed-45e3-a6fc-ac1fbe223b2f',
                            'd77ee136-0ded-4cca-87f0-a779bb6cd012',
                            'd805c05f-78c3-4c42-b144-ed85a2452267',
                            'd8385387-e6a1-434c-a83d-64dccbbebe9b',
                            'd88b6641-b78f-41e9-b00b-0cd60004c27b',
                            'de798944-4d9b-4893-9a3b-cfceadf390a1',
                            'e5b6fb4f-0419-4185-bf42-05874b1ece79',
                            'e5fee47d-79ca-4cc4-95c2-f432f0f62379',
                            'e9d8e1c7-089d-4c27-87b7-a411f223d013',
                            'ea1c8352-a843-492d-b119-0dc4fdabe630',
                            'eaabfaeb-9e39-46a4-8b24-3f25fa5758fc',
                            'edbe3080-065b-4994-90cd-6d00b7799039',
                            ]
        self.read_data_dir()

    def read_data_dir(self):
        for stream_dir in os.scandir(self.data_dir):
            if stream_dir.is_dir():
                owner = stream_dir.path[-36:]
                if self.users=="all":
                    self.scan_stream_dir(stream_dir)
                elif owner in self.lab_participants:
                        self.scan_stream_dir(stream_dir)

    def scan_stream_dir(self, stream_dir):
        for day_dir in os.scandir(stream_dir.path):
            if day_dir.is_dir():
                for stream_dir in os.scandir(day_dir):
                    if stream_dir.is_dir():
                        stream_dir = stream_dir.path
                        tmp = stream_dir.split("/")[-3:]
                        owner_id  = tmp[0]
                        day = tmp[1]
                        stream_id = tmp[2]
                        files_list = []
                        dir_size = 0
                        for f in os.listdir(stream_dir):
                            if f.endswith(".gz"):
                                new_filename = (stream_dir+"/"+f).replace(self.data_dir,"")
                                files_list.append(new_filename)
                                dir_size += os.path.getsize(stream_dir+"/"+f)
                        metadata_filename = self.data_dir+files_list[0].replace(".gz", ".json")
                        with open(metadata_filename, 'r') as mtd:
                            metadata = mtd.read()
                        metadata = json.loads(metadata)
                        stream_name = metadata["name"]
                        self.sqlData.add_to_db(owner_id, stream_id, stream_name, day, files_list, dir_size, metadata)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='CerebralCortex Data Replay')
    parser.add_argument('-users','--users', help='Scan all users directories or only for the list provided in the script.', type=str, default="all", required=False)
    parser.add_argument('-conf','--conf', help='CerebralCortex configuration file', required=True)

    args = vars(parser.parse_args())

    with open(args["conf"]) as ymlfile:
        config = yaml.load(ymlfile)


    ReplayCerebralCortexData(args["users"],config)
