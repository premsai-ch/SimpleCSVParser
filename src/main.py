from FileRead import create_byte_streams
from multiprocessing import Pool
import os

#FILE = "worldcitiespop.txt"
FILE = "pagecounts.txt"

if __name__ == '__main__':  
  filesz = os.path.getsize(FILE)>>20
  arr = [i*10485760*4 for i in xrange((filesz+40)/40)]
  pool = Pool(processes=8)
  pool_outputs = pool.map(create_byte_streams,arr)
  pool.close()
  pool.join()
