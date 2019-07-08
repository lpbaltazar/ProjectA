import warnings
warnings.filterwarnings("ignore")

import os
import time
from utils import toCSV, getDownloaded
from quali import getQualitative
from quanti import getQuantitative, getDate
from multiprocessing import Pool
from azure.datalake.store import core, lib, multithread

month = 'december'
download_dir = '../downloaded'
data_dir = 'ProdDataHub/TransactionFactTable/IWant/2018/12'

def main(file, f):
	s = time.time()
	quali_out = os.path.join("../../data/quali/"+month, f[-12:])
	quanti_out = os.path.join("../../data/quanti/"+month, f[-12:])
	month_out = os.path.join("../../data/month/"+month, f[-12:])
	pool = Pool()
	df = pool.apply(getQualitative, args = (file, ["gigyaid", "devicetype", "deviceos", "browsertype",
															"connectivitytype", "devicename", "mobiledevice",
															"screensize", "videoquality", "ipaddress"]))
	toCSV(df, quali_out)
	df = pool.apply(getQuantitative, args = (file, ["gigyaid", "viewpageduration", "pagedepth", "actiontaken", "videotitle", "bigdatasessionid"]))
	toCSV(df, quanti_out)
	df = pool.apply(getDate, args = (file, ["gigyaid", "bigdatasessionid", "sessionstarttimestamp", "sessionendtimestamp", "viewpageduration"]))
	toCSV(df, month_out)
	pool.close()
	pool.join()
	e = time.time()
	total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
	print("Total process time: ", total_time)

if __name__ == '__main__':
	token = lib.auth()
	adl = core.AzureDLFileSystem(token, store_name = 'bigdatadevdatalake')
	downloaded = getDownloaded()
	print("Downloaded files: ", downloaded)
	for f in adl.ls(data_dir):
		if f in  downloaded: continue
		s = time.time()
		print("Processing file {}".format(f[-38:]))
		outfile = os.path.join(download_dir, f[-38:])
		downloader = multithread.ADLDownloader(adl, f, outfile)
		if downloader.successful():
			print("Finished Downloading!")
			main(outfile, f)
			os.remove(outfile)
			e = time.time()
			total_time = time.strftime("%H:%M:%S", time.gmtime(e-s))
			print("Finished downloading: ", total_time)
			with open("downloaded.txt", "a") as myfile:
				myfile.write(f)
		else:
			print("error in downloading!")
	# main('../downloaded/IWantTransactionFactTable-20181201.csv', 'ProdDataHub/TransactionFactTable/IWant/2018/12/IWantTransactionFactTable-20181201.csv')
	# for f in os.listdir("../10"):
	# 	file = os.path.join("../10", f)
	# 	main(file, f)
	# 	os.remove(file)