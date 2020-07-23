import gzip
import json
import os
from datetime import datetime

root_dir = '/tmp/sendgrid'


def fact(n: int) -> int:
    if n <= 1:
        return n
    return n * fact(n - 1)


def traverse(dir):
    for dir_name, sub_dir_last, file_list in os.walk(dir):
        print('Found directory: %s' % dir_name)
        for file_name in file_list:
            if file_name != 'rep.sh':
                print('\t%s' % file_name)
                path = "/".join([dir_name, file_name])
                converted = []
                with gzip.open(path, 'rb') as fin:
                    messages = fin.readlines()
                    for message_line in messages:
                        message = json.loads(message_line.decode('utf-8'))
                        message.update({'last_event_time': datetime.timestamp(
                            datetime.strptime(message['last_event_time'], '%Y-%m-%dT%H:%M:%SZ'))})
                        converted.append(message)
                with gzip.open(path, 'wb') as out:
                    if converted:
                        for line in converted:
                            out.write(line)


f = fact(5)
traverse(root_dir)
