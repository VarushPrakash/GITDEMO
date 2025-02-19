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

"""calculate_area(3,5)"""

def contact(name,number,city):
    print(f"{name} from {city} contact number: {number}")

contact("Varush",654,"Madurai")

def area_cal(length,breadth):
    """length=float(input("Enter length: "))
    breadth=float(input("Enter breadth: "))"""

    area=length*breadth
    print(area)
    area_cal(7,7)


def addno(a,b):
    result = a+b
    return result


def cal_rectangle(length,width):
    area= length*width
    perimeter=2*(length*width)
    return area,perimeter

rectangle_area,rect_perimeter=cal_rectangle(2,2)

print(rect_perimeter)
print(rectangle_area)








