def con(s):
    res = ''
    for i in range(len(s)):
        if i % 2 != 0:
            res += s[i].upper()
        else:
            res += s[i]
    return res


s = input("Enter the string: ")
print(con(s))