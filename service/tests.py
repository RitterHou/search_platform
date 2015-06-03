# Create your tests here.
class ClassTest(object):
    def __init__(self):
        self.set_a('init_a')

    def set_a(self, a):
        self.a = a

    def set_b(self, b):
        self.b = b


test = ClassTest()
print test.a
# print test.b
test.set_b('set_b')
print test.b