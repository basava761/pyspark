s=input('enter  the string :')

n=len(s)
str=''
import string
for i in range(n):
    if i%2==0:
       str=str+s[i].upper()
    else:
       str=str+s[i]

print(str)
print(string.ascii_lowercase)
print(string.ascii_uppercase)
print(string.ascii_letters)
