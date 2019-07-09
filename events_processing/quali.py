import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import re
import os
import time
import pandas as pd
import numpy as np

from date import removeNotLoggedIn
from preprocessing.utils import readChunk, toCSV

data_dir = "../../events/2018/12"
file = 'IWantVideoEvent-20181201.csv'

def getQualitative(data_dir, file):
	s = time.time()
	cols = ["USERID", "PRIMARY_FINGERPRINT", "ISMOBILE", "USER_OS", "APP_TAG", "THREE_G_TAG", "CONTENT_TYPE", "IPCITY", "USER_HOSTADDRESS", "MOBILE_DEVICE"]
	df = readChunk(os.path.join(data_dir, file), cols)
	df["USERID"] = df["USERID"].astype(str)
	df["PRIMARY_FINGERPRINT"] = df["PRIMARY_FINGERPRINT"].astype(str)
	print("all users: ", len(df))
	df = removeNotLoggedIn(df)

	df.rename(columns = {"ISMOBILE":"DEVICETYPE", "APP_TAG":"ACCESSTYPE", "THREE_G_TAG":"CONNECTIVITY_TYPE"}, inplace = True)
	df["DEVICETYPE"] = df["DEVICETYPE"].apply(lambda x: "MOBILE" if x == "1" else "PC")
	df["ACCESSTYPE"] = df["ACCESSTYPE"].apply(lambda x: "MOBILE APPLICATION" if x == "1" else "WEB APPLICATION")
	df["CONNECTIVITY_TYPE"] = df["CONNECTIVITY_TYPE"].apply(lambda x: "DATA" if x in ["2G", "3G", "4G LTE", "4G"] else x)
	df["USER_OS"] = df["USER_OS"].astype(str).apply(lambda x: x.upper())
	df["USER_OS"] = df["USER_OS"].apply(lambda x: "ANDROID" if re.search(r'ANDROID', x) else x)
	df["USER_OS"] = df["USER_OS"].apply(lambda x: "IOS" if re.search(r'IOS', x) else x)
	df["USER_OS"] = df["USER_OS"].apply(lambda x: "WINDOWS" if re.search(r'WINDOWS', x) else x)
	df["USER_OS"] = df["USER_OS"].apply(lambda x: "MAC OS" if re.search(r'MAC OS', x) else x)
	df["CONTENT_TYPE"] = df["CONTENT_TYPE"].astype(str).apply(lambda x: x.upper())
	print(len(df.index.unique()))
	group = df.groupby("USERID")
	devicetype = group.apply(lambda x: x["DEVICETYPE"].unique().tolist()).reset_index(name="DEVICETYPE")
	deviceos = group.apply(lambda x: x["USER_OS"].unique().tolist()).reset_index(name="USER_OS")
	ipaddress = group.apply(lambda x: x["USER_HOSTADDRESS"].unique().tolist()).reset_index(name="USER_HOSTADDRESS")
	browsertype = group.apply(lambda x: x["ACCESSTYPE"].unique().tolist()).reset_index(name="ACCESSTYPE")
	connectivitytype = group.apply(lambda x: x["CONNECTIVITY_TYPE"].unique().tolist()).reset_index(name="CONNECTIVITY_TYPE")
	content_type = group.apply(lambda x: x["CONTENT_TYPE"].unique().tolist()).reset_index(name="CONTENT_TYPE")
	mobiledevice = group.apply(lambda x: x["MOBILE_DEVICE"].unique().tolist()).reset_index(name="MOBILE_DEVICE")
	ipcity = group.apply(lambda x: x["IPCITY"].unique().tolist()).reset_index(name="IPCITY")

	df = pd.concat([devicetype, deviceos, ipaddress, browsertype, connectivitytype, mobiledevice, ipcity], axis = 1)
	df = df.loc[:, ~df.columns.duplicated()]
	df = df.set_index('USERID')
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Finish getting qualitative features: ", total_time)
	return df

def main(data_dir, out_dir):
	s = time.time()
	for f in os.listdir(data_dir):
		if f == ".DS_Store": continue
		print(f)
		df = getQualitative(data_dir, f)
		toCSV(df, os.path.join(out_dir, f[-12:]))
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Finish month processing: ", total_time)
if __name__ == '__main__':
	main("../../events/2018/12", "../../events/quali/december")