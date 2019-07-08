import warnings
warnings.filterwarnings("ignore")

import os
import time
import pandas as pd
import numpy as np

from utils import readCSV, toCSV


value = lambda x: x.strip("[]").replace("'", "").split(", ")
converters={"devicetype": value,
			"deviceos": value,
			"ipaddress": value,
			"browsertype": value,
			"connectivitytype": value,
			"screensize": value,
			"videoquality": value,
			"devicename": value,
			"mobiledevice": value}
def getUnique(df, col):
	group = df.groupby("gigyaid")
	df[col] = df[col].apply(tuple)
	unique = group.apply(lambda x: x[col].unique()).reset_index(name=col)
	unique[col] = unique[col].apply(lambda a: list(set([x for t in a for x in t])))
	return unique

def getQualiMonth(month):
	all_df = []
	for f in os.listdir('../../data/quali/'+month):
		df = readCSV(os.path.join('../../data/quali/'+month, f), converters = converters)
		all_df.append(df)

	all_df = pd.concat(all_df)
	all_df.set_index("gigyaid", inplace = True)
	cols = all_df.columns
	new_df = pd.DataFrame(index = all_df.index.unique(), columns = cols)
	for col in cols:
		all_df[col] = all_df[col].apply(lambda x: [i.upper() for i in x])
		new_df[col] = getUnique(all_df, col)[col].values
	new_df.index.name = "gigyaid"

	toCSV(new_df, "../../data/aggregated/quali"+month+".csv")

def getSum(df, col):
	group = df.groupby("gigyaid")[col].sum().to_frame()
	return group

def getContentsUnique(df, col):
	# df[col] = df[col].astype(str)
	group = df.groupby("gigyaid")
	contents = group.apply(lambda x: x["contentswatched"].unique().tolist()).reset_index(name="contentswatched")
	return contents

def getQuantiMonth(month):
	all_df = []
	for f in os.listdir('../../data/quanti/'+month):
		df = readCSV(os.path.join('../../data/quanti/'+month, f), dtype = str)
		all_df.append(df)

	all_df = pd.concat(all_df)
	all_df.set_index("gigyaid", inplace = True)
	cols = all_df.columns
	new_df = pd.DataFrame(index = all_df.index.unique(), columns = cols)
	for col in cols:
		if col == "contentswatched":
			# print(getContentsUnique(all_df, col))
			contents = getContentsUnique(all_df, col)
			new_df = pd.merge(new_df, contents, left_index=True, right_on='gigyaid')
			new_df.drop("contentswatched_x", axis = 1, inplace = True)
			new_df.rename({"contentswatched_y":"contentswatched"}, axis = 1, inplace = True)
		else:
			all_df[col] = all_df[col].astype(float)
			new_df[col] = getSum(all_df, col)[col].values
	toCSV(new_df, "../../data/aggregated/quanti"+month+".csv")

def main():
	s = time.time()
	month = "october"
	getQualiMonth(month)
	getQuantiMonth(month)
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Total aggregate time: {} for the month of {}".format(total_time, month))

if __name__ == '__main__':
	main()