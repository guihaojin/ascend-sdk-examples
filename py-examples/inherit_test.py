# Broken
# Traceback (most recent call last):
#   File "inherit_test.py", line 14, in <module>
#     print(c.foo(1, 2, 3))
# TypeError: foo() takes at most 3 arguments (4 given)

class Base:
    def foo(a, b):
        raise 

class Child(Base):
    def foo(a, b, c=None):
        return a + b


if __name__ == '__main__':
    c = Child()
    print(c.foo(1, 2, 3))
