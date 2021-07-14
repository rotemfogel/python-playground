class Parent:
    def __init__(self, report: bool = False):
        self.report = report

    def execute(self):
        print(self.report)


class Child(Parent):
    def __init__(self, *args, **kwargs):
        super(Child, self).__init__(*args, **kwargs)


if __name__ == "__main__":
    Child().execute()
    Child(report=True).execute()
    Child(report=False).execute()
