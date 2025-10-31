import random

#name = input("What is your name? ")

prediction = ["You will become a billionaire ", "You will lose all of your hair ", "You will be successful "]


for i in range(3):
    name = input("What is your name? ")
    print(name + " " + random.choice(prediction))
    