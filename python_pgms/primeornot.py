n=int(input('Enter the number to validate:'))

if n<2:
    print('not a prime number')
else:
    c=0
    for i in range(1,n+1):
        if n%i==0:
            c=c+1
            

if c==2:
    print(n,'is a prime number')
else:
    print(n,'is not a prime')
