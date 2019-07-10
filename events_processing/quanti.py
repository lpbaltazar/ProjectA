import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import os
import time
import pandas as pd
import numpy as np

from preprocessing.utils import readChunk

data_dir = "../../events/2018/12"
file = 'IWantVideoEvent-20181201.csv'

df = readChunk(os.path.join(data_dir, file))

# columns = ["NumSessions", "NumWatched", "TotalWatchingDuration", "CLICK_COUNT", "ADPLAY_COUNT", "PLAY_COUNT", "PAUSE_COUNT", "RESUME_COUNT", "AB_FLAG", "SEEK_COUNT", "BUFFER_COUNT", "TOTAL_BUFFER_TIME", "MAX_BUFFER_TIME", "Completion_Rate"]
columns = ["NumSessions", "NumWatched", "TotalWatchingDuration", "CLICK_COUNT", "ADPLAY_COUNT", "PLAY_COUNT", "PAUSE_COUNT", "RESUME_COUNT", "AB_FLAG", "SEEK_COUNT", "BUFFER_COUNT", "TOTAL_BUFFER_TIME", "MAX_BUFFER_TIME", "Completion_Rate"]
new_df = pd.DataFrame(index = df.USERID.unique(), columns = columns)
new_df["NumSessions"] = df.groupby("USERID")["SESSIONID"].nunique()
new_df["NumWatched"] = df.groupby("USERID")["VIDEO_CATEGORY_TITLE"].nunique()
# new_df["TotalWatchingDuration"] = 