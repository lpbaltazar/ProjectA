import warnings
warnings.filterwarnings('ignore')

import os
import time

import pandas as pd
import numpy as np

from utils import readCSV

def getQualitative(file, usecols):
	s = time.time()
	print("Getting the qualitative features: ", file)
	transact = readCSV(file, usecols)
	transact = transact.loc[transact.gigyaid.notnull()]
	transact.loc[transact.browsertype.notnull(), "browsertype"] = "WEB APPLICATION"
	transact.browsertype.replace(np.nan, "MOBILE APPLICATION", inplace = True)
	if len(transact) == 0:
		return pd.DataFrame()
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("No unique customer")
	else:
		group = transact.groupby("gigyaid")
		devicetype = group.apply(lambda x: x["devicetype"].unique().tolist()).reset_index(name="devicetype")
		deviceos = group.apply(lambda x: x["deviceos"].unique().tolist()).reset_index(name="deviceos")
		ipaddress = group.apply(lambda x: x["ipaddress"].unique().tolist()).reset_index(name="ipaddress")
		browsertype = group.apply(lambda x: x["browsertype"].unique().tolist()).reset_index(name="browsertype")
		connectivitytype = group.apply(lambda x: x["connectivitytype"].unique().tolist()).reset_index(name="connectivitytype")
		screensize = group.apply(lambda x: x["screensize"].unique().tolist()).reset_index(name="screensize")
		videoquality = group.apply(lambda x: x["videoquality"].unique().tolist()).reset_index(name="videoquality")
		devicename = group.apply(lambda x: x["devicename"].unique().tolist()).reset_index(name="devicename")
		mobiledevice = group.apply(lambda x: x["mobiledevice"].unique().tolist()).reset_index(name="mobiledevice")
		df = pd.concat([devicetype, deviceos, ipaddress, browsertype, connectivitytype, screensize, videoquality, devicename, mobiledevice], axis = 1)
		df = df.loc[:, ~df.columns.duplicated()]
		df = df.set_index('gigyaid')
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("Finish getting qualitative features: ", total_time)
		return(df)