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
# List of files to read
files = ["train.csv"]

allow_writting = False
#read the files 
all_objects = read_files(files, comm, rank, size)

start_time_programm = time.time()
if rank == 0 :
    attributes = ['age', 'workclass', 'education', 'salary', 'occupation', 'gender', 'hoursperWeek', 'country', 'relationship']
    #print("Time of the whole Program is : ", time.time() - start_time )
    num_data = len(all_objects)
    time_line0  =  0 
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

try:
    while True:

        if rank == 0:
            time_line_whole = time.time()
            print("Ablauf time : ", time_line_whole - time_line0)
            print("----------------------------------------- \n")
            print("********  Menu  ********")
            print("1. Filter objects by attributes")
            print("2. Calculate average / or frequency of an attribute")
            print("3. Sort objects by attribute")
            #print("4. Write results to file")
            print("4. Exit")
            print("----------------------------------------- \n")
            choice = int(input("Enter your choice: "))
        else:
            choice = 0
        
        choice = comm.bcast(choice, root=0)

        if choice == 3:
            timeOfSorting = time.time()
            if rank == 0:
                attr = input("Enter the attribute to sort by: ")
            else:
                attr = ""
            attr = comm.bcast(attr, root=0)
            #sorted_objects = sort_by_age(objects)
            sorted_objects = sort_by_attribute(objects, attr)
            #print("Rank " , rank, " has sorted the data")
            comm.barrier()
            gathered_sorted_objects = comm.gather(sorted_objects, root=0)   
            # sort objects by attr
            if rank == 0: 
                gathered_sorted_objects = [obj for sublist in gathered_sorted_objects for obj in sublist]
                # for obj in gathered_sorted_objects:
                #     obj.printInfos()
                
                write_to_file = input("Do you want to write the results to a file? (y/n): ")
                if write_to_file == "y":
                    allow_writting = True
                else:
                    allow_writting = False
                #print("Time of the whole Sorting Task is : ", time.time() - timeOfSorting )
            
            allow_writting = comm.bcast(allow_writting, root=0)
            if (allow_writting == True):

                filename = "sorted_objects.txt"
                #file = MPI.File.Open(comm, filename, MPI.MODE_CREATE | MPI.MODE_WRONLY | MPI.MODE_APPEND)
                comm.barrier()
                fh = MPI.File.Open(comm, "sorted_objects.txt", \
                        MPI.MODE_RDWR | MPI.MODE_CREATE, \
                        MPI.INFO_NULL)

                status = MPI.Status()

                #print("rank {} and length {}".format(rank, len(sorted_objects)))

                offset = comm.rank * len(sorted_objects)
                #fh.Seek(offset, MPI.SEEK_SET)
                str_test = '\n'.join([obj.p_tostring(comm.rank) for obj in sorted_objects])


                buf = str_test.encode('ascii')
                #     #print(buf)
                    
                fh.Write_shared(buf)  
                
                fh.Close()
            
        elif choice == 1:
            #timeOfFiltering = time.time()
            if rank == 0:
                print("Choose : ", attributes)
                #attr = input("Enter the attribute to filter by: ")
                choosenAttributes = []
                values = []
                while True:
                    attr = input("Enter the attribute name (or 'done' to finish): ")
                    if attr == "done":
                        break
                    #attr = input("Enter the attribute name: ")
                    value = input("Enter the value: ")
                    if attr == "age" or attr == "hoursperWeek" or attr == "salary" :
                        value = int(value)
                    
                    choosenAttributes.append(attr)
                    values.append(value)
                attr_values = dict(zip(choosenAttributes, values))
                timeOfFiltering = time.time()
            else:
                # attr = 0
                # value = 0 
                choosenAttributes = []
                values = []
                attr_values = {}
                attr = ""
                       
            attr_values = comm.bcast(attr_values, root=0)
            filtered_objects = filter_by_attributes_normal(objects, attr_values)
            filtered_objects_all_processes = comm.gather(filtered_objects, root=0)
            #Zeitpunkt1 = time.time()-timeOfFiltering
            #print("TEST: " , Zeitpunkt1)
            #all_Zeitpunkt1 = comm.gather(Zeitpunkt1, root=0)
            #print("Rank = {}  Filter: Zeitpunkt1: object gefiltert und gesammelt in root: {}".format(Zeitpunkt1 ))
            if rank == 0:
                #filtered_objects_all_process = comm.gather(filtered_objects,root=0)
                #print("Until here is working")
                    # Flatten the list of lists
                filtered_objects_all_processes = [obj for sublist in filtered_objects_all_processes for obj in sublist]
                Zeitpunkt1 = time.time()-timeOfFiltering
                sum = 0 
                # for obj in all_Zeitpunkt1:
                #     sum = sum + obj
                    #print(type(obj.salary))
                print("Rank = {}  Filter: Gesamte Zeitpunkt1: object gefiltert {}".format(rank, Zeitpunkt1 ))
                time_line0 = time.time() 
                #print("Number of filtered objects:", len(filtered_objects_all_processes))
                allow_writting = True
                # write_to_file = input("Do you want to write the results to a file? (y/n): ")
                # if write_to_file == "y":
                #     allow_writting = True
                # else:
                #     allow_writting = False
                # print("Time of the whole Filtering Task is : ", time.time() - timeOfFiltering )
            Zeitpunkt2=time.time()
            
            allow_writting = comm.bcast(allow_writting, root=0)
            if (allow_writting == True):

                filename = "{}_filtered_objects.txt".format(attr_values['age'])
                #file = MPI.File.Open(comm, filename, MPI.MODE_CREATE | MPI.MODE_WRONLY | MPI.MODE_APPEND)
                comm.barrier()
                fh = MPI.File.Open(comm, filename, \
                        MPI.MODE_RDWR | MPI.MODE_CREATE, \
                        MPI.INFO_NULL)

                status = MPI.Status()

                #print("rank {} and length {}".format(rank, len(filtered_objects)))

                offset = comm.rank * len(filtered_objects)
                #fh.Seek(offset, MPI.SEEK_SET)
                str_test = '\n'.join([obj.p_tostring(comm.rank) for obj in filtered_objects])
                buf = str_test.encode('ascii')
                #     #print(buf)
                    
                fh.Write_shared(buf)   
                
                fh.Close()
                print("Write rank: {} Filter: Zeitpunkt2 schreiben von der Daten: {}".format(rank, time.time()-Zeitpunkt2))
            

        elif choice == 2:
            #timeOfCalculating = time.time() 
            if rank == 0:
                attr = input("Enter the attribute to calculate average of: ")
                timeOfCalculating = time.time()             
            else:
                attr = ""
            
            attr = comm.bcast(attr, root=0)

            if attr == "age":
                attr_values_calc = [obj.age for obj in objects]
                #print('attr_value:  ' , attr_values_calc)
                local_sum_age = __builtins__.sum(attr_values_calc)  
                global_sum = comm.reduce(local_sum_age, op=MPI.SUM, root=0)
            elif attr == "salary":
                attr_values_calc = [obj.salary for obj in objects]
                local_sum_salary = __builtins__.sum(attr_values_calc)  
                global_sum = comm.reduce(local_sum_salary, op=MPI.SUM, root=0)
            elif attr == "hoursperWeek":
                attr_values_calc = [obj.hoursperWeek for obj in objects]
                local_sum_hours = __builtins__.sum(attr_values_calc)  
                global_sum = comm.reduce(local_sum_hours, op=MPI.SUM, root=0)
            
            else:
                groups = calculate_frequency(objects, attr)
                #print(groups)
                gathered_groups = comm.gather(groups, root=0)
              
            if rank == 0:
                if attr == "age" or attr == "salary" or attr == "hoursperWeek":
                    total_num_objects = len(all_objects)
                    average = global_sum / total_num_objects
                    print("The average of attribute {} is: {}".format(attr, average))
                else:
                    #gathered_groups = {k: v for group in gathered_groups for k, v in group.items()}

                    
                    combined_groups = defaultdict(lambda: {"count": 0})
                    for group in gathered_groups:
                        for country, count in group.items():
                            if country and not (country == "nan" or country == None):
                                combined_groups[country]["count"] += count['count']
                            #combined_groups[country] += count["count"] 
                    #print(" {}".format(combined_groups))
                    x_values = list(combined_groups.keys())
                    #print(type(x_values))
                    y_values = [val['count'] for val in combined_groups.values()]
                    #print(type(y_values))
                    #save_asfig(x_values, y_values, attr)
                    save_asfig_test(x_values,y_values, attr)
                    # save_asfig(combined_groups, attr)
                    # for value, group in combined_groups.items():
                    #     print(f"{attr}: {value}, Count: {group['count']}")           
                print("rank {}  Time of the whole Task  Calculating is : {} ".format(rank, time.time() - timeOfCalculating) )    

        elif choice == 4:
            # exit program
            print("Rank {} Time of the whole All Tasks is : {}".format(rank, time.time() - start_time_programm ))
            break

        else:
            print("Invalid choice. Please enter a valid choice.")


except KeyError as e:
    print("key error: ", e)
    comm.Abort()