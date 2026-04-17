# Write a Program To REVERSE content of the given String by using slice operator

s=input('Enter te string:')

print(s[::-1])

print("===============================================================================")
print("===============================================================================")

r=''
i=len(s)-1
while i>=0:
    r=r+s[i]
    i=i-1
print(r,'by second method')

print("===============================================================================")
print("===============================================================================")

r1=reversed(s)
print(''.join(r1))

