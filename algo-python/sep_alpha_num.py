s=input('enter alpha num: ')
alpha=[]
num=[]
sp=[]
for c in s:
    if c.isalpha():
        alpha.append(c)
    elif c.isnumeric():
        num.append(c)
    else:
        sp.append(c)


print(alpha)
print(num)
print(sp)