from numba import jit
from io import open
#from os import lseek
import os
import itertools as itr
from multiprocessing import cpu_count, Pool
#import time


FILE = "worldcitiespop.txt"
#FILE = "pagecounts.txt"

def count_line():
  b = bytearray(10485760*2)
  f = open(FILE,"rb+")
  count=0
  for i in itr.count():
    numread = f.readinto(b)
    if not numread:
      break
    for j in xrange(len(b)):
      if(b[j]==10):
        count+=1
  print('Number of lines in '+FILE+"  "+str(count))



def create_byte_streams(offset):
  b = bytearray(10485760*2)
  f = open(FILE, "rb")
  f.seek(offset,0)
  numread = f.readinto(b)
  creating_byte_lists(b)
  f.close()

def creating_byte_lists(byte_stream):
  start = time.time()
  count,j=0,0
  lines=[]
  for byte in byte_stream:
    j+=1;
    if(byte == 10):
      stream=byte_stream[count:j]
      count,count2,line,m = j+1,0,[],0
      for k in stream:
        m+=1
        if(k==32 or k==44):
          word = stream[count2:m]
          count2 = m+1
          line.append(word)
      lines.append(line)
  end = time.time()
  print("Time Taken: "+str(end-start))
  print("Lines added: "+str(len(lines)))


if __name__ == '__main__':
  print(str(cpu_count())+" CPUs available")
  filesz = os.path.getsize(FILE)>>20
  arr = [i*10485760*2 for i in xrange((filesz+20)/20)]
  pool_size = 8
  pool = Pool(processes=pool_size)
  pool_outputs = pool.map(create_byte_streams,arr)
  pool.close()
  pool.join()

