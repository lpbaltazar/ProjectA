import warnings
warnings.filterwarnings("ignore")

import os
import time
import pandas as pd
import numpy as np
from utils import readCSV

if __name__ == '__main__':
	file = '../10/IWantTransactionFactTable-20181031.csv'
	cols = ["gigyaid", "bigdatasessionid", "sessionstarttimestamp", "sessionendtimestamp", "viewpageduration"]
	df = pd.read_csv(file, usecols = cols)
	df = df.loc[df.viewpageduration.notnull()]
	# df["viewpageduration"] = df["viewpageduration"].replace(np.nan, 0)
	df["viewpageduration"] = df["viewpageduration"].astype(int)
	group = df.groupby(["gigyaid", "bigdatasessionid", "sessionstarttimestamp", "sessionendtimestamp"])["viewpageduration"].sum().to_frame()
	print(group)