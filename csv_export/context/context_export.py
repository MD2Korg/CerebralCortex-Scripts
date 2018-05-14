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


import argparse
import uuid
import numpy as np

from cerebralcortex.cerebralcortex import CerebralCortex

def all_users_data(study_name: str, csv_output_dir, CC):
    # get all participants' name-ids
    all_users = CC.get_all_users(study_name)

    if all_users:
        for user in all_users:
            print("Processing {} - {}".format(user["identifier"], user["username"]))
            process(user["identifier"], user["username"], csv_output_dir, CC)
    else:
        print(study_name, "- study has 0 users.")


def process(user_id: uuid, user_name: str, csv_output_dir, CC: CerebralCortex):
    context_where = get_csv_data(user_id, user_name, "org.md2k.data_analysis.feature.context.context_where", CC)
    context_interaction = get_csv_data(user_id, user_name, "org.md2k.data_analysis.feature.context.context_interaction",
                                       CC)
    context_activity_engaged = get_csv_data(user_id, user_name,
                                            "org.md2k.data_analysis.feature.context.context_activity_engaged", CC)

    write_csv_file(csv_output_dir, "context_where.csv", context_where)
    write_csv_file(csv_output_dir, "context_interaction.csv", context_interaction)
    write_csv_file(csv_output_dir, "context_activity_engaged.csv", context_activity_engaged)


def get_csv_data(user_id, user_name, stream_name, CC):
    stream_ids = CC.get_stream_id(user_id, stream_name)
    csv_header = "user_name, SurveySentDate, SurveySentTime, StartDate, EndDate, SurveyType, agreeableness.d"
    csv_pattern = "%s,%s,%s,%s,%s,%s,%s"
    csv_data = ""
    for stream_id in stream_ids:
        stream_id = stream_id["identifier"]
        days = CC.get_stream_days(stream_id)

        for day in days:
            #print(5*"-", "Stream ID {} - Day {}".format(stream_id, day))
            data = CC.get_stream(stream_id=stream_id, user_id=user_id, day=day).data
            for dp in data:
                # write data to CSV file
                # user_name, SurveySentDate, SurveySentTime, StartDate, EndDate, SurveyType, agreeableness.d
                answer = np.nonzero(dp.sample)[0][0]+1
                csv_data += csv_pattern % (user_name.replace("mperf_",""), "", "", dp.start_time, dp.end_time, "", answer)+"\n"
                #print(csv_data)

    return csv_header + "\n" + csv_data


def write_csv_file(file_path, file_name, data):
    if file_path[-1] != "/":
        file_path += "/"
    with open(file_path + file_name, "w") as csv_file:
        csv_file.write(data)


if __name__ == '__main__':
    # create and load CerebralCortex object and configs
    parser = argparse.ArgumentParser(description='CerebralCortex CSV Exporter.')
    parser.add_argument("-conf", "--cc_config_filepath", help="Configuration file path", required=True)
    parser.add_argument("-op", "--output_path", help="Output path where all CSV files would be stored.", required=True)

    args = vars(parser.parse_args())

    CC = CerebralCortex(args["cc_config_filepath"])

    # run for all the participants in a study
    all_users_data("mperf", args["output_path"], CC)
