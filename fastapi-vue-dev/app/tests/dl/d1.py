class SingleClass(object):
    __instance = None  # 自定义一个类属性做判断

    def __new__(cls, *args, **kwargs):
        if cls.__instance == None:
            '''
            如果__instance为空，证明是第一次创建实例
            通过父类的__new__方法创建实例
            '''
            cls.__instance = object.__new__(cls)
            return cls.__instance
        else:
            # 返回上一个对象的引用
            return cls.__instance


def Singleton(cls):
    _instance = {}

    def _singleton(*args, **kargs):
        if cls not in _instance:
            _instance[cls] = cls(*args, **kargs)
        return _instance[cls]

    return _singleton


@Singleton
class A(object):
    a = 1

    def __init__(self, x=0):
        self.x = x


#
# a1 = A(2)
# a2 = A(3)


class Singleton2(type):
    def __init__(cls, name, bases, dict):
        super(Singleton2, cls).__init__(name, bases, dict)
        cls._instance = None

    def __call__(cls, *args, **kw):
        if cls._instance is None:
            cls._instance = super(Singleton2, cls).__call__(*args, **kw)
        return cls._instance


class MyClass3(object):
    __metaclass__ = Singleton2


class SingletonType(type):
    def __init__(self, *args, **kwargs):
        super(SingletonType, self).__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):  # 这里的cls，即Foo类
        print('cls', cls)
        obj = cls.__new__(cls, *args, **kwargs)
        cls.__init__(obj, *args, **kwargs)  # Foo.__init__(obj)
        return obj


class Foo(metaclass=SingletonType):  # 指定创建Foo的type为SingletonType
    pass
    # def __init__(self, name):
    #     self.name = name

    # def __new__(cls, *args, **kwargs):
    #     return object.__new__(cls)


if __name__ == '__main__':
    # a = SingleClass()
    # a.__setattr__("a", 2)
    # b = SingleClass()
    # print(a.__getattribute__("a"))
    # print(b.__getattribute__("a"))
    a1 = A(2)
    a2 = A()
    print(a1.x)
    print(a2.x)
