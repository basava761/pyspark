#sum of 3 numbers
n= list(map(int, input("Enter numbers: ").split()))
print(n)
num=len(n)
c=0

for i in n:
    c=c+i


print(f'sum of {num} number is {c}')
