from io import open


#FILE = "worldcitiespop.txt"
FILE = "pagecounts.txt"

def create_byte_streams(int offset):
  b = bytearray(10485760*4)
  f = open(FILE, "rb")
  f.seek(offset,0)
  numread = f.readinto(b)
  creating_byte_lists(b)
  f.close()

def creating_byte_lists(byte_stream):
  cdef int count,j,k,count2,m
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
  print("Lines added: "+str(len(lines)))



