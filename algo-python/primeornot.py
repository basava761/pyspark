def is_prime(n):
    c=0
    if n>2:
        for i in range(2,n):
            if n%i==0:
                c=c+1

        if c==2:
            print(f'{n}\t is a prime number')
        else:
            print(f'{n}\t is not a prime number')
    else:
        print('not a prime')


n=int(input('enter the number: '))

is_prime(n)