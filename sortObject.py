import math
from mpi4py import MPI


attributes = ['age', 'workclass', 'education', 'salary', 'occupation', 'gender', 'hoursperWeek', 'country', 'relationship']



def sort_by_age(objects):
    sorted_objects = sorted(objects, key=lambda x: x.age)
    return sorted_objects


def sort_by_salary(objects, salary_threshold):
    sorted_objects = sorted(filter(lambda x: x.salary > salary_threshold, objects), key=lambda x: x.salary)
    return sorted_objects

 
def filter_by_attribute(objects, attr, value):
    if attr in attributes:
        return [obj for obj in objects if getattr(obj, attr) == value]
    else:
        print("Invalid attribute")
    return []


def filter_by_multiple_attributes(objects, attr1, val1, attr2, val2):
    filtered_objects = filter_by_attribute(objects, attr1, val1)
    filtered_objects = filter_by_attribute(filtered_objects, attr2, val2)
    return filtered_objects




def filter_by_attributes_normal(objects, attr_values):
    filtered_objects = []
    for obj in objects:
        match = True
        for attr, value in attr_values.items():
            if getattr(obj, attr) != value:
                match = False
                break
            if attr == "salary":
                print("Attri : {} and {}".format(value, obj.salary))
        if match:           
            filtered_objects.append(obj)
    return filtered_objects

def sort_by_attribute_(objects, attribute):
    return sorted(objects, key=lambda obj: getattr(obj, attribute))






def filter_by_attributes(objects, attr_values):
    filtered_objects = []
    for obj in objects:
        for attr, value in attr_values.items():
            if attr not in attributes:
                raise ValueError("Invalid attribute: " + attr)
            if type(value) != type(getattr(obj, attr)):
                raise ValueError("Invalid value type for attribute " + attr)
            if str(getattr(obj, attr)).casefold() == str(value).casefold():
                filtered_objects.append(obj)
                break
    return filtered_objects

def sort_by_attribute(objects, attr):
    if attr == "age":
        sorted_objects = sorted(objects, key=lambda obj: obj.age)
    elif attr == "salary":
        sorted_objects = sorted(objects, key=lambda obj: obj.salary)
    elif attr == "workclass":
        sorted_objects = sorted(objects, key=lambda obj: obj.workclass.casefold())
    elif attr == "gender":
        sorted_objects = sorted(objects, key=lambda obj: obj.gender.casefold())
    elif attr == "country":
        sorted_objects = sorted(objects, key=lambda obj: obj.country.casefold())
    elif attr == "occupation":
        sorted_objects = sorted(objects, key=lambda obj: obj.occupation.casefold())
    elif attr == "education":
        sorted_objects = sorted(objects, key=lambda obj: obj.education.casefold())
    elif attr == "hoursperWeek":
        sorted_objects = sorted(objects, key=lambda obj: obj.hoursperWeek)  
    elif attr == "relationship":
        sorted_objects = sorted(objects, key=lambda obj: obj.relationship.casefold())  
    
    else:
        print("Attribute not found")
        sorted_objects = []

    return sorted_objects 



def calculate_frequency(objects, attr):

    groups = {}

    for obj in objects:
        if attr == "country":
            value = obj.country
        elif attr == "gender":
            value = obj.gender
        elif attr == "education":
            value = obj.education
        elif attr == "occupation":
            value = obj.occupation
        elif attr == "workclass":
            value = obj.workclass
        elif attr == "relationship":
            value = obj.relationship
        else:
            print("Invalid attribute. Please enter a valid attribute.")
            return groups
        #print("value : " , type(value))

        if isinstance(value, float) and math.isnan(value):
            #print(type(value))
            value = "not defined"
            #print(value)

        
        if value in groups:
            groups[value]["count"] += 1
        else:
            groups[value] = {"count": 1}

    return  groups
