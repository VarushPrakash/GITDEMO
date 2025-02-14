def add():
    num1=float(input("Enter the first number"))
    num2=float(input("Enter the second number"))
    num3=str(input())
    result=num1+num2
    print(result)
    print(f"{num3} scored {result} marks")

def sub():
    num1=float(input("Enter the first number: "))
    num2=float(input("Enter the second number: "))
    result = num1-num2
    print(result)

def multiply(a,b):
    print(a*b)

def calculate_area(l,b):
    print(l)
    print(b)
    area=l*b
    print(f"{l}*{b}={area}")

calculate_area(3,5)

def contact(name,number,city):
    print(f"{name} from {city} contact number: {number}")

contact("Varush",654,"Madurai")




