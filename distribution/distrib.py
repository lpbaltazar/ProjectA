import warnings
warnings.filterwarnings("ignore")

import sys
sys.path.append("../")

import matplotlib as mpl
mpl.use('TkAgg')

import os
import time
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from preprocessing.utils import readCSV

month = "december"
data_dir = os.path.join("../../data/quanti", month)
file = "20181201.csv"

converters = {'gigyaid':str, 'contentswatched':str}
df = readCSV(os.path.join(data_dir, file), dtype = int, converters = converters)
print(df.shape)

plt.bar(df.gigyaid.values)
plt.savefig('try.png')