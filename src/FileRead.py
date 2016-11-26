#from numba import jit
from io import open
#from os import lseek
import os
import itertools as itr
from multiprocessing import cpu_count, Pool, Process
import time
import cProfile


#FILE = "worldcitiespop.txt"
FILE = "pagecounts.txt"


def do_cprofile(func):
  def profiled_func(*args, **kwargs):
    profile = cProfile.Profile()
    try:
      profile.enable()
      result = func(*args, **kwargs)
      profile.disable()
      return result
    finally:
      profile.print_stats()
  return profiled_func

'''
try:
  from line_profiler import LineProfiler
  def do_profile(follow=[]):
    def inner(func):
      def profiled_func(*args, **kwargs):
        try:
          profiler = LineProfiler()
          profiler.add_function(func)
          for f in follow:
            profiler.add_function(f)
          profiler.enable_by_count()
          return func(*args, **kwargs)
        finally:
          profiler.print_stats()
      return profiled_func
    return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner
'''

def count_line():
  count,line_offset,offset = 0,{},0
  line_offset[0]=offset
  with open(FILE,"rb+",os.O_NONBLOCK) as f:
    for line in f:
      count+=1
      offset += len(line)
      if(count%50000==0):
        line_offset[count]=offset
  print('Number of lines in '+FILE+"  "+str(count))
  return (count,line_offset)

 
#@do_cprofile
def creating_byte_lists(byte_stream):
  count,j=0,0
  #lines=[]
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
      #lines.append(line)
  #print("Lines added: "+str(len(lines)))

@do_cprofile
def parsing_lines(linenorange):
  linenostart,linenoend,offset = linenorange
  count=0
  byte_stream=bytearray("")
  with open(FILE,"rb+",os.O_NONBLOCK) as file:
    file.seek(offset)
    count=linenostart
    for line in file:
      count+=1
      if(count==linenoend):
        break
      if(count>=linenostart):
        byte_stream.extend(line)
  count,j=0,0
  for byte in byte_stream:
    j+=1
    if(byte == 10):
      stream=byte_stream[count:j]
      count,count2,line,m = j+1,0,[],0
      for k in stream:
        m+=1
        if(k==32 or k==44):
          word = stream[count2:m]
          count2 = m+1
          line.append(word) 

if __name__ == '__main__':
  k,line_offset = count_line()
  cpus = cpu_count()
  linc = k/cpus
  lineranges = [(n, min(n+linc, k),line_offset[n]) for n in xrange(0,k,linc)]
  #print(lineranges)
  pool = Pool(cpus)
  pool.imap_unordered(parsing_lines,lineranges)
  pool.close()
  pool.join()