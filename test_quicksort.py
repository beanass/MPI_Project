
import time
import pandas as pd
from mpi4py import MPI
import io
from MPIReadFile import *
from models.PersonClass import Person
import numpy
from collections import Counter
from collections import defaultdict
from sortObject import *



comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
offset = 0 

files = ["test_train.csv"]

allow_writting = False
 
all_objects = read_files(files, comm, rank, size)


if rank == 0 :
    attributes = ['age', 'workclass', 'education', 'salary', 'occupation', 'gender', 'hoursperWeek', 'country', 'relationship']
    #print("Time of the whole Program is : ", time.time() - start_time )
    num_data = len(all_objects)
else:
    num_data = None

try:
    num_data = comm.bcast(num_data, root=0)
    counts = [num_data // size] * size
    remainder = num_data % size
    
    for i in range(remainder):
       counts[i] += 1
    offsets = [sum(counts[:i]) for i in range(size)]
    #print("####### Process {} offset : {}".format(rank, offsets))
    start_idx = offsets[rank]
    end_idx = start_idx + counts[rank]
    #print("####### Process {} Start index is {} and End index is {}".format(rank, start_idx, end_idx))
    if rank == 0:
        objects = all_objects[start_idx:end_idx]
        for i in range(1, size):
           comm.send(all_objects[offsets[i]:offsets[i]+counts[i]], dest=i)
    else:
        objects = comm.recv(source=0)
except:
    comm.Abort()
if rank == 0:
    attr = input("Enter the attribute to sort by: ")
else:
    attr = ""
attr = comm.bcast(attr, root=0)

local_array = []
sorted_objects = sort_by_attribute(objects, attr)
local_tmp = []
final_sorted = []
#local_array.sort()

for step in range(0, size):
    print ("Step: ", step)
    if (step % 2 == 0):
        if (rank % 2 == 0):
            des = rank + 1
        else:
            des = rank - 1
    else:
        if (rank % 2 == 0):
            des = rank - 1
        else:
            des = rank + 1
            
    if (des >= 0 and des < size):
        print ("My rank is ", rank, " and my des is ", des)
        comm.send(sorted_objects, dest = des, tag = 0)
        local_tmp = comm.recv(source = des)    
        print ("Rank ", rank, " ", step, ": Initial local_array: ", len(sorted_objects))
        print ("Rank ", rank, " ", step, ": Received local_tmp:", len(local_tmp))
        sorted_objects.extend(local_tmp)
        local_remain = sort_by_attribute(sorted_objects, attr)
        print(local_remain)
        if (rank < des):
            sorted_objects = local_remain[0:int(num_data/size)]
        else:
            sorted_objects = local_remain[int(num_data/size):2 * int(num_data/size)]
        print ("Rank ", rank, " ", step, ": Retained portions: ", sorted_objects)

final_sorted = comm.gather(sorted_objects, root=0)
print(type(final_sorted))
if (rank  == 0):
    for obj in final_sorted:
        obj.printInfos()