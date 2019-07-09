import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import re
import os
import time
import pandas as pd
import numpy as np

from preprocessing.utils import readChunk, toCSV


cols = ["USERID", "SESSIONID", "SESSION_STARTDT", "SESSION_ENDDT"]
def removeNotLoggedIn(df):
	df["loggedin"] = df[["USERID", "PRIMARY_FINGERPRINT"]].apply(lambda x: 0 if re.search(x[1], x[0]) else 1, axis = 1)
	df = df.loc[df.loggedin == 1]
	print("logged in users: ", len(df))
	return df

def main(data_dir, outdir):
	s = time.time()
	count = 0
	for f in os.listdir(data_dir):
		print("Extracting cols for: ", f)
		df = readChunk(os.path.join(data_dir, f))
		df["USERID"] = df["USERID"].astype(str)
		df["PRIMARY_FINGERPRINT"] = df["PRIMARY_FINGERPRINT"].astype(str)
		print("all users: ", len(df))
		df = removeNotLoggedIn(df)
		df = df[cols]
		outfile = os.path.join(outdir, f[-12:])
		toCSV(df, outfile)
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Finish extracting all cols: ", total_time)

if __name__ == '__main__':
	main("../../events/2018/12", "../../events/rfm/december")
	main("../../events/2019/01", "../../events/rfm/january")
	main("../../events/2019/02", "../../events/rfm/february")
	main("../../events/2019/03", "../../events/rfm/march")
	main("../../events/2019/04", "../../events/rfm/april")
	main("../../events/2019/05", "../../events/rfm/may")
	main("../../events/2019/06", "../../events/rfm/june")
# userid = "9479A9C164A4C32676CBD502BE91664B_175.158.211.49"
# primary_fingerprint = "9479A9C164A4C32676CBD502BE91664B"

# if re.search(primary_fingerprint, userid):
# 	print(0)
# else:
# 	print(1)