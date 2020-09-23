import pandas as pd
import numpy as np
import pickle

desired_width=320
pd.set_option('display.width', desired_width)
np.set_printoptions(linewidth=desired_width)
pd.set_option('display.max_columns',10)

with open("/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata.pkl", "rb") as f:
    data = f.read()
    data = pickle.loads(data)
df = pd.DataFrame(data)
print("dd")
# df.to_parquet('/Users/ali/IdeaProjects/MD2K_DATA/hdfs_mismatch_columns/mismatched_metadata.parquet')
# print("read")