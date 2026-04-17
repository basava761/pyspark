def rev_num(n):
    r=0
    while n!=0:
        r1= n%10
        rem=r+r1
        n=n//10
        print(rem,end=' ')

n=int(input('enter the number: '))
rev_num(n)
