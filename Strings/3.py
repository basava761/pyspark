#| Q5. Write a Program To REVERSE internal content of each word?

str=input('Enter the string:')
l=[]
s=str.split(' ')
for i in s:
    l.append(i[::-1])

print(l)
print(' '.join(l))

