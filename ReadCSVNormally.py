import csv
import time

from sortObject import calculate_frequency

start_time = time.time()

from models.PersonClass import Person

personList = []

with open('train.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
        personList.append(Person(row[0], row[1], row[2], row[3], row[6], row[7], row[9], row[12], row[13]))

personList.pop(0)
print("Count of Objects is : %d", len(personList))
print("--- %s seconds ---" % (time.time() - start_time))

# for obj 8
# in personList:
#     str_test = obj.writePerson()


combined_groups = calculate_frequency(personList, 'country')

print(type(combined_groups))