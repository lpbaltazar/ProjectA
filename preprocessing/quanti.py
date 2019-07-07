import warnings
warnings.filterwarnings("ignore")

import os
import time

import pandas as pd
import numpy as np
from utils import readChunk

def getQuantitative(file, usecols):
	s = time.time()
	print("Getting the quantitative features: ", file)
	transact = readChunk(file, usecols)
	transact = transact.loc[transact.gigyaid.notnull()]
	if len(transact) == 0:
		return pd.DataFrame()
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("No unique customer")
	else:
		transact = transact.loc[transact.viewpageduration.notnull()]
		transact["viewpageduration"] = transact["viewpageduration"].astype(int)
		totalviewpageduration = transact.groupby("gigyaid")["viewpageduration"].sum().to_frame()
		totalnumbersession = transact.groupby("gigyaid")["bigdatasessionid"].nunique().to_frame()
		quanti = pd.concat([totalviewpageduration, totalnumbersession], axis = 1)
		quanti = quanti.loc[:, ~quanti.columns.duplicated()]

		actions = list(set(transact["actiontaken"].unique().tolist()))
		for action in actions:
			temp = transact.loc[transact["actiontaken"] == action]
			quanti[action] = temp.groupby("gigyaid")["actiontaken"].count()

		temp = transact.loc[transact["actiontaken"].notnull()]
		quanti["watched"] = temp.groupby("gigyaid")["videotitle"].nunique()
		quanti["contentswatched"] = temp.groupby("gigyaid")["videotitle"].unique().tolist()
		quanti.fillna(0, inplace=True)
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("Finish getting quantitative features: ", total_time)
		return quanti

def getDate(file, usecols):
	s = time.time()
	print("Getting the time features: ", file)
	transact = readChunk(file, usecols)
	transact = transact.loc[transact.gigyaid.notnull()]
	if len(transact) == 0:
		return pd.DataFrame()
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("No unique customer")
	else:
		transact = transact.loc[transact.viewpageduration.notnull()]
		transact["viewpageduration"] = transact["viewpageduration"].astype(int)
		group = transact.groupby(["gigyaid", "bigdatasessionid", "sessionstarttimestamp", "sessionendtimestamp"])["viewpageduration"].sum().to_frame()
		e = time.time()
		total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
		print("Finish getting date features: ", total_time)
		return group