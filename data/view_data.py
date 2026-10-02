#%%
import pandas as pd
from config.paths import VOLUME_DATA_PATH

READ_PATH = VOLUME_DATA_PATH/"volume_data_1y.parquet"

if __name__ == "__main__":
    df = pd.read_parquet(READ_PATH)
    df.head(10)