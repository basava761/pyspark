a=int(input('enter the number for a:'))
b=int(input('enter the number for b:'))
c=int(input('enter the number for c:'))
d=int(input('enter the number for d:'))
print(a,b,c,d)

x=lambda a,b,c,d:a if a>b and a>c and a>d else b if b>c and b>d else c if c>d else d
print(x(a,b,c,d),'is the largest value among')