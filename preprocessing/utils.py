import warnings
warnings.filterwarnings("ignore")

import os
import time
import numpy as np
import pandas as pd

def readCSV(file, usecols, converters = None, encoding = None):
	df = pd.read_csv(file, usecols = usecols, dtype = str, 
		low_memory = False, converters = converters, encoding = encoding)
	return df

def toCSV(df, filename, index = True, encoding = None):
	df.to_csv(filename, index = index, sep = ',', encoding = encoding)
