from numba import jit
from io import open
#from os import lseek
import os
import itertools as itr
from multiprocessing import cpu_count, Pool, Process
#import time
import cProfile


FILE = "worldcitiespop.txt"
#FILE = "pagecounts.txt"


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






def count_line():
  count=0
  with open(FILE,"rb+") as f:
    for line in f:
      count+=1
  print('Number of lines in '+FILE+"  "+str(count))
  return count


@do_cprofile
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


if __name__ == '__main__':
  count=0
  k = count_line()
  linc = k/cpu_count()
  procs = []
  with open(FILE,"rb+") as f:
    for line in f:
      count+=1
      if(count==1):
        b=bytearray(line)
      else:
        b.extend(line)
      if(count==linc):
        count=0
        p = Process(target=creating_byte_lists,args=(b,))
        procs.append(p)
        p.start()
  if(count>=1):
    p = Process(target=creating_byte_lists,args=(b,))
    procs.append(p)
    p.start()




