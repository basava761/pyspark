#Write a Program To REVERSE order of words present in the given string

str=input('enter the string:')
s=str.split(' ')
print (s)
s1=s[::-1]
print(s1)
s2=' '.join(s1)
print(s2)