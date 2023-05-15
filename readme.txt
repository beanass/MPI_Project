The Program is done to read different csv files with different processes using mpi4py 
the requierement are to install mpi4py , pandas,  matplotlib, numpy librairies to be able to use it.


to use the programm enter : ( example for 4 processes )
mpirun -n 4 python main_project.py

After executing the programm a Menu will apear. 
Chose a number of the menu and enter the attribut choice from the following : 
(age, workclass, salary, education, occupation,  relationship, gender, hoursperWeek, country)
and then enter the value of the choosen attribut. 

for the first choise and thirth one you have the possibility to write the result into a file.
The file will appear in the same folder as the project.

for the second choise, the avearage can be calculate just for : age, salary and hoursperWeek
for the other attributes the frequency will be calculated instead and a graph will saved at the same 
folder. 
