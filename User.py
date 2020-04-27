import json
from typing import List


class Student(object):
    def __init__(self, first_name: str, last_name: str, age: int = None):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age


class Team(object):
    def __init__(self, students: List[Student], name: str):
        self.students = students
        self.name = name


student1 = Student(first_name="Jake", last_name="Doyle")
student2 = Student(first_name="Jake", last_name="Doyle")
print(student1.last_name)
print(student1.age)

team = Team([student1, student2], "yoni")
team_json = json.dumps(team, default=lambda o: o.__dict__, indent=4)
# print(team_json)
decoded_team = Team(**json.loads(team_json))
print(decoded_team.students[0])
print(decoded_team.students)
print(decoded_team.name)