import json

from airflow import settings
from airflow.models import Variable
from airflow.models.crypto import get_fernet

fernet = get_fernet()
session = settings.Session()
vars = session.query(Variable).all()
backup = {}
for var in vars:
    parts = str(var).split(":")
    trimmed = list(map(lambda x: x.strip(), parts))
    key = trimmed[0]
    value = fernet.decrypt(bytes(str(trimmed[1:]), "utf-8")).decode()
    backup.update({key: value})

print(json.dumps(backup))
