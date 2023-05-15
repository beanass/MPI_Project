import random
import csv
def generate_random_data():
    data = []
    age = random.randint(17, 65)
    workclass = random.choice(["Private", "Government", "Self-Employed"])
    fnlwgt = random.randint(30000, 200000)
    education = random.choice(["High School", "Bachelors", "Masters", "PhD"])
    educational_num =  random.choice(['3', '20'])
    marital_status = random.choice(["Divorced", "Never-married", "Married-civ-spouse", "Widowed", ])
    occupation = random.choice(["Engineer", "Teacher", "Doctor", "Lawyer", "Artist"])
    relationship = random.choice(["Married", "Single", "Divorced"])
    race  =random.choice(["_", "__"])
    gender = random.choice(["Male", "Female"])
    capital_gain = random.choice(['0', '99999', '11929', '2863', '23421'])
    capital_loss =random.choice(['0', '10000', '2342', '3243', '2423'])
    #c  =random.randint(0, 1)
    hoursperWeek = random.randint(20, 60)
    native_country = random.choice(["USA", "UK", "India", "Germany", "France", "Mexico", "Philippines", "Germany"])
    income_ = random.choice(['0', '1'])

    return [age, workclass, fnlwgt, education, educational_num, marital_status, occupation, relationship, race, gender, capital_gain, capital_loss, hoursperWeek, native_country,income_ ]

def write_data_to_csv(filename):
    with open(filename, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["age", "workclass", "fnlwgt" , "education", "educational-num", "marital-status","occupation", "relationship", "race", "gender", "capital-gain" , "capital-loss","hours-per-week", "native-country", "income_>50k" ])
        for i in range(30000):
            data = generate_random_data()
            writer.writerow(data)
for i in range(0, 24):
    write_data_to_csv("random_data_{}.csv".format(i + 4))