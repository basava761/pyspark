import numpy as np

l=np.arange(1,10)
print(l)
l1=l
print(type(l),'printing l',l)
print(type(l1),'printing l1',l1)
l1[0]=100
print(type(l),'printing l',l)
print(type(l1),'printing l1',l1)
l1=l.copy()
print(type(l),'printing l',l)
print(type(l1),'printing l1',l1)
l1[0]=1000
print(type(l),'printing l',l)
print(type(l1),'printing l1',l1)    