import struct

# Construct objects w/ fields corresponding to columns.
# Store fields using the appropriate representation:
  # TEXT => bytes
  # DATE => bytes
  # INTEGER => int
  # FLOAT => float

class Lineitem(object):
  # The format string, for use with the struct module.
  fmt = "4I4fss10s10s10s25s10s44s"
  # Initialize the columns in Lineitem with proper representation.
  l_orderkey = int
  l_partkey = int
  l_suppkey = int
  l_linenumber = int
  l_quantity = float
  l_extendedprice = float
  l_discount = float
  l_tax = float
  l_returnflag = bytes
  l_linestatus = bytes
  l_shipdate = bytes
  l_commitdate = bytes
  l_receiptdate = bytes
  l_shipinstruct = bytes
  l_shipmode = bytes
  l_comment = bytes

  # Initialize a lineitem object.
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):
    self.l_orderkey = int(args[0]) 
    self.l_partkey = int(args[1])
    self.l_suppkey = int(args[2])
    self.l_linenumber = int(args[3])
    self.l_quantity = float(args[4])
    self.l_extendedprice = float(args[5])
    self.l_discount = float(args[6])
    self.l_tax = float(args[7])
    self.l_returnflag = bytes(args[8])
    self.l_linestatus = bytes(args[9])
    self.l_shipdate = bytes(args[10])
    self.l_commitdate = bytes(args[11])
    self.l_receiptdate = bytes(args[12])
    self.l_shipinstruct = bytes(args[13])
    self.l_shipmode = bytes(args[14])
    self.l_comment = bytes(args[15])

  # Pack this lineitem object into a bytes object.
  def pack(self):
    return struct.pack(self.fmt, self.l_orderkey, self.l_partkey, self.l_suppkey, self.l_linenumber, self.l_quantity, self.l_extendedprice, self.l_discount, self.l_tax, self.l_returnflag, self.l_linestatus, self.l_shipdate, self.l_commitdate, self.l_receiptdate, self.l_shipinstruct, self.l_shipmode, self.l_comment)


  # Construct a lineitem object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    temp = struct.unpack(cls.fmt, byts)
    new_input = []
    for i in temp:
      if isinstance(i, str):
        new_input.append(i.split('\x00')[0])
      else:
        new_input.append(i)
    return Lineitem(new_input[0], new_input[1], new_input[2], new_input[3], new_input[4], new_input[5], new_input[6], new_input[7], new_input[8], new_input[9], new_input[10], new_input[11], new_input[12], new_input[13], new_input[14], new_input[15])

  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

    
class Orders(object):
  # The format string, for use with the struct module.
  fmt = "2Isf10s15s15sI79s"

  # Initialize the columns 
  o_orderkey = int
  o_custkey = int
  o_orderstatus = bytes
  o_totalprice = float
  o_orderdate = bytes
  o_orderpriority = bytes
  o_clerk = bytes
  o_shippriority = int
  o_comment = bytes

  # Initialize an orders object.
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):
    self.o_orderkey = int(args[0])
    self.o_custkey = int(args[1])
    self.o_orderstatus = bytes(args[2])
    self.o_totalprice = float(args[3])
    self.o_orderdate = bytes(args[4])
    self.o_orderpriority = bytes(args[5])
    self.o_clerk = bytes(args[6])
    self.o_shippriority = int(args[7])
    self.o_comment = bytes(args[8])

  # Pack this orders object into a bytes object.
  def pack(self):
    return struct.pack(self.fmt, self.o_orderkey, self.o_custkey, self.o_orderstatus, self.o_totalprice, self.o_orderdate, self.o_orderpriority, self.o_clerk, self.o_shippriority, self.o_comment)


  # Construct an orders object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    temp = struct.unpack(cls.fmt, byts)
    new_input = []
    for i in temp:
      if isinstance(i, str):
        new_input.append(i.split('\x00')[0])
      else:
        new_input.append(i)
    return Orders(new_input[0], new_input[1], new_input[2], new_input[3], new_input[4], new_input[5], new_input[6], new_input[7], new_input[8])
  
  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

# Return a list of 'cls' objects.
# Assuming 'cls' can be constructed from the raw string fields.
def readCsvFile(inPath, cls, delim='|'):
  lst = []
  with open(inPath, 'r') as f:
    for line in f:
      fields = line.strip().split(delim)
      lst.append(cls(*fields))
  return lst

# Write the list of objects to the file in packed form.
# Each object provides a 'pack' method for conversion to bytes.
def writeBinaryFile(outPath, lst):
  raise NotImplementedError()

# Read the binary file, and return a list of 'cls' objects.
# 'cls' provicdes 'byteSize' and 'unpack' methods for reading and conversion.
def readBinaryFile(inPath, cls):
  raise NotImplementedError()
