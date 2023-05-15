
import array
import time
import pandas as pd
from mpi4py import MPI
import io
from models.PersonClass import Person
import matplotlib.pyplot as plt

# class DataObject:
#     def __init__(self, file_name, data):
#         self.file_name = file_name
#         self.data = data

#     def process(self):
#         # Do something with the data
#         print("Processing data from file {}:".format(self.file_name))
#         print(self.data.head())

#     def __str__(self):
#         return "File name: {}\nData:\n{}\n".format(self.file_name, self.data.head())


def read_files(files, comm, rank, size):
    
    # Each process reads a file or more depending on the number of files
    file_count = len(files)

    # Determine the number of files each process should read 
    chunk_size = file_count // size
    start = rank * chunk_size
    end = start + chunk_size

    # The last rank should read the remaining files, in case of an odd number of files
    if rank == size - 1:
        end = file_count

    # Each process reads its designated files and create objects with the data
    objects = []
    for file_name in files[start:end]:
        # start measuring the time for each Process 
        start_time = time.time()
        data = pd.read_csv(file_name, header = 0)
        #print(data.head(0))

        try:
            data = data.rename(columns={"age": "age","fnlwgt": "salary", "hours-per-week": "hoursperWeek",  "native-country": "country"})
            for index, row in data.iterrows():
                obj = Person(row['age'], row['workclass'], row['salary'], row['education'], row['occupation'], row['relationship'], row['gender'], row['hoursperWeek'], row['country'])
                #obj.printInfos() 
                objects.append(obj)

        except KeyError as e:
            print("key error: ", e)
            comm.Abort() # abort all processes if an error occur, to avoid infinit loops 
        
        # calculate the time to read a files by each process 
        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Elapsed time for Process {rank} to read the data is : {elapsed_time}")
                        
    # Gather all the objects from each process into a single list on rank 0
    all_objects = comm.gather(objects, root=0)

    # Create a list to store the objects
    objects_list = []

    if rank == 0:
        # Flatten the list of lists
        try:
            all_objects = [obj for sublist in all_objects for obj in sublist]
            print("Number of all Person is: ", len(all_objects))
            # counts = [len(all_objects) // size] * size
            # for i in range(len(all_objects) % size):
            #     counts[i] += 1
            # offsets = [sum(counts[:i]) for i in range(size)]
            # comm.Scatterv(all_objects, [objects, MPI.OBJECT], counts, offsets, root=0)
        except KeyError as e:
            print("key error: ", e)
            comm.Abort()
    else:
        all_objects = None 
    return all_objects
        #for obj in all_objects:
            #print(obj)
        #print("Time of the whole Program is : ", time.time() - start_time )


def save_asfig_new(x_values, y_values, attr):
    # attr_map = {"country", "gender", "education", "occupation", "workclass", "relationship"}
    # if attr in attr_map:
    #     value = attr
    # else:
    #     value = "Invalid attribute"
    # Create the bar chart
    #fig = plt.figure()
    #ax = fig.add_axes(array(len(x_values)))
    #ax.bar(x_values, y_values)
    x_axis = ['value_1', 'value_2', 'value_3']
    y_axis = ['value_1', 'value_2', 'value_3']
    # Add labels and title
    #ax.xlabel(attr)
    plt.bar(x_axis, y_axis)
    plt.ylabel("Frequency")
    plt.title("Frequency of Different")
    plt.show()
    # Save the graph to a file
    #ax.savefig("_frequency")

def save_asfig_test(x_axis, y_axis, attr):

    #x_axis = ['value_1', 'value_2', 'value_3']
    #y_axis = ['value_1', 'value_2', 'value_3']

    plt.bar(x_axis, y_axis)
    plt.ylabel("Frequency")
    plt.title("Frequency of Different " + attr)
    #plt.xticks([item for item in range (len(x_axis))])
    plt.xticks(rotation=90)
    plt.xticks(fontsize=5)
    plt.savefig(attr + "__frequency.png")


def ask_for_writting():
    print("-------------------------------------- ")
    print("Do you want to write the result in a file Y/N:  ")
    answer = " "
    while answer not in ["Yes", "yes", "Y", "y", "No", "no", "N", "n"]:
        answer = input("Enter the attribute to calculate average of: ") 
