l1 = [1, 2, 3, 4]
l2 = ['a', 'b', 'c', 'd']
d1 = zip(l1, l2)
print(d1)  # Output:<zip object at 0x01149528>
# Converting zip object to dict using dict() contructor.
print(dict(d1))
l1 = ['a', 'b', 'c', 'd']
d1 = dict(enumerate(l1))
print(d1)  # Output:{0: 'a', 1: 'b', 2: 'c', 3: 'd'}

from collections import Counter

c1 = Counter(['c', 'b', 'a', 'b', 'c', 'a', 'b'])
# key are elements and corresponding values are their frequencies
print(c1)  # Output:Counter({'b': 3, 'c': 2, 'a': 2})
print(dict(c1))  # Output:{'c': 2, 'b': 3, 'a': 2}


# data = [dict(zip(result.keys(), result)) for result in results]