import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import os
import time
import pandas as pd
import numpy as np
from preprocessing.utils import readChunk, getUnique, toCSV

# month = "december"
# data_dir = os.path.join("../../events/quali", month)
# data_dir = "../../events/quali/december"

value = lambda x: x.strip("[]").replace("'", "").split(", ")
converters={"DEVICETYPE": value,
			"USER_OS": value,
			"USER_HOSTADDRESS": value,
			"ACCESSTYPE": value,
			"CONNECTIVITY_TYPE": value,
			"MOBILE_DEVICE": value,
			"IPCITY": value}

def aggregateQuali(data_dir, month):
	s = time.time()
	all_df = []
	for f in os.listdir(data_dir):
		all_df.append(readChunk(os.path.join(data_dir, f), converters = converters))

	all_df = pd.concat(all_df)
	all_df = all_df.set_index("USERID")
	print("Total Number of Rows: ", len(all_df))
	print("Total Number of Unique Customers: ", len(all_df.index.unique()))
	group = all_df.groupby("USERID")
	cols = all_df.columns
	new_df = pd.DataFrame(index = group.groups.keys(), columns = cols)
	for col in cols:
		print(col)
		new_df[col] = getUnique(all_df, col, group)[col].values

	new_df.index.name = "USERID"
	# print(new_df.head(10))
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Total process time for {} is {}".format(month, total_time))
	return new_df

def main(month):
	print("PROCESSING FOR MONTH {}".format(month))
	data_dir = os.path.join("../../events/quali", month)
	outfile = "../../events/quali/aggregated/"+month+".csv"
	toCSV(aggregateQuali(data_dir, month), outfile)
	print("\n\n\n")

if __name__ == '__main__':
	main("december")
	main("january")
	main("february")
	main("march")
	main("april")
	main("may")