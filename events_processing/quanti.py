import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import os
import time
import pandas as pd
import numpy as np

from date import removeNotLoggedIn
from preprocessing.utils import readChunk, toCSV

data_dir = "../../events/2018/12"
file = 'IWantVideoEvent-20181201.csv'

def getQuantitative(data_dir, file):
	s = time.time()
	df = readChunk(os.path.join(data_dir, file))
	df.dropna(subset = ["USERID"], inplace = True)
	df["USERID"] = df["USERID"].astype(str)
	df["PRIMARY_FINGERPRINT"] = df["PRIMARY_FINGERPRINT"].astype(str)
	print("all users: ", len(df))
	df = removeNotLoggedIn(df)
	columns = ["USR_ACT_TOT_WATCHING_DUR", "VIDEO_DURATION", "CLICK_COUNT", "ADPLAY_COUNT", "PLAY_COUNT", "PAUSE_COUNT", "RESUME_COUNT", "AB_FLAG", "SEEK_COUNT", "BUFFER_COUNT", "TOTAL_BUFFER_TIME",
				"SESSIONID", "VIDEO_CATEGORY_TITLE", "VIDEO_CONTENT_TITLE"]
	new_df = pd.DataFrame(index = df.USERID.unique(), columns = columns)

	sum_cols = ["USR_ACT_TOT_WATCHING_DUR", "VIDEO_DURATION", "CLICK_COUNT", "ADPLAY_COUNT", "PLAY_COUNT", "PAUSE_COUNT", "RESUME_COUNT", "AB_FLAG", "SEEK_COUNT", "BUFFER_COUNT", "TOTAL_BUFFER_TIME"]
	for col in sum_cols:
		print(col)
		df[col] = df[col].astype(float)
		new_df[col] = df.groupby("USERID")[col].sum()

	nunique_cols = ["SESSIONID", "VIDEO_CATEGORY_TITLE", "VIDEO_CONTENT_TITLE"]
	for col in nunique_cols:
		print(col)
		new_df[col] = df.groupby("USERID")[col].nunique()

	new_df["MAX_BUFFER_TIME"] = df.groupby("USERID")["MAX_BUFFER_TIME"].max()
	df["CONTENT_TYPE"].fillna("NAN", inplace = True)
	content_types = list(df["CONTENT_TYPE"].unique())
	print(content_types)
	for content in content_types:
		print(content)
		temp = df.loc[df.CONTENT_TYPE == content]
		new_df[content] = temp.groupby("USERID")["CONTENT_TYPE"].count()
		new_df[content].fillna(0, inplace = True)

	new_df.index.name = "USERID"
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Finish processing: ", total_time)
	return new_df

def main(data_dir, out_dir):
	s = time.time()
	for f in os.listdir(data_dir):
		if f == ".DS_Store": continue
		print(f)
		df = getQuantitative(data_dir, f)
		toCSV(df, os.path.join(out_dir, f[-12:]))
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Finish month processing: ", total_time)

if __name__ == '__main__':
	main("../../events/2018/12", "../../events/quanti/december")
	main("../../events/2019/01", "../../events/quanti/january")
	main("../../events/2019/02", "../../events/quanti/february")
	main("../../events/2019/03", "../../events/quanti/march")
	main("../../events/2019/04", "../../events/quanti/april")
	main("../../events/2019/05", "../../events/quanti/may")