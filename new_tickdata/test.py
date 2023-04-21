# d = {}
# print(d.keys())
# print(list(d.keys()))
# for i in d.values():
#     print(i)
prices = [1, 2, 3, 4, 5, 6]
p1 = []
p2 = []
for p in prices:
    for k in prices:
        if k >= p:
            p1.append(k)
        if k <= p:
            p2.append(k)
print(p1)
print(p2)
