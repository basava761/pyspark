l=[1,2,6,8,0,8,7,4,11]
max=l[0]
n=len(l)

for i in range(1,n):
    if l[i]>max:
        max=l[i]


print(f'maximum value is {max}')