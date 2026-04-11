n=int(input('Enter the number: '))

n1=0
n2=1

for i in range(2,n):
    sum=n1+n2
    n2=n1

    n1=sum
    print(sum)


