# Write a Program To REVERSE internal content of every second word present in the given string

str=input('enter  the string to rev:: ')
s=str.split(' ')
l=[]

for i in range(len(s)):
    if i%2!=0:
        l.append(s[i][::-1])
    else:
        l.append(s[i])

print(l)
print(' '.join(l))
