class Person:
    age = 0
    workclass = ""
    education = ""
    salary = 0 
    occupation = ""
    gender = ""
    hoursperWeek = 0
    country = ""
    relationship = ""

    def __init__(self, age, workclass, salary, education, occupation,  relationship, gender, hoursperWeek, country):
        self.age = age   
        self.workclass = workclass
        self.education = education
        self.salary = salary
        self.occupation = occupation
        self.gender = gender
        self.hoursperWeek = hoursperWeek
        self.country = country
        self.relationship = relationship
    
    def printInfos(self):
        print(" age: {0}, workclass: {1}, salary: {2}, education: {3}, occupation: {4},  relationship: {5}, gender: {6}, hoursperWeek: {7}, country{8}"
        .format(self.age,self.workclass, self.salary, self.education, self.occupation,  self.relationship, self.gender, self.hoursperWeek, self.country))

    def p_tostring(self, rank):
        return "Rank "+ str(rank)+" age: " + str(self.age) + " salary: " + str(self.salary) + " hoursperWeek: " + str(self.hoursperWeek) + " gender: " + str(self.gender) + " country: " + str(self.country) +  "\n"


def createPersonlist(reader):
    personList = []
    for row in reader:
        print(row)
        personList.append(Person(row[0], row[1], row[2], row[3], row[6], row[7], row[9], row[12], row[13]))
    
    return personList        
        
