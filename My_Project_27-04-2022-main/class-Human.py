class human:
    name = None
    age = None

    def get_name(self):
        self.name = input("please enter your name: ")

    def get_age(self):
        self.age = input("please enter your age: ")

    def put_name(self):
        print("my name is",self.name)

    def put_age(self):
        print("my name is", self.age)


person1 = human()
person1.get_name(),
person1.get_age()
person1.put_name(),person1.put_age()
