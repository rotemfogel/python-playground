import os
import time

from typing import List


def _get_logs_dir() -> str:
    return '/data/rotem/Downloads/installs'


def _list_dir(path: str) -> List[str]:
    prefix = f'{path}/' if not path.endswith('/') else path
    paths = [f.path for f in os.scandir(prefix) if f.is_dir()]
    return ','.join(paths)


def _clean_dir(paths: str, upper_bound: int) -> None:
    def delete(p: str) -> None:
        print(f'about to delete {p}')
        os.remove(p)

    for root in paths.split(','):
        for path, sub_directories, _ in os.walk(root):
            if os.stats(path).st_mtime < upper_bound:
                delete(path)
            else:
                dirs_to_check = list(map(lambda x: os.path.join(path, x), sub_directories))
                dirs_to_delete = [f for f in dirs_to_check if os.stat(f).st_mtime < upper_bound]
                for dir_to_delete in dirs_to_delete:
                    delete(dir_to_delete)


if __name__ == "__main__":
    logs_dir = _get_logs_dir()
    dirs = _list_dir(logs_dir)
    watermark = time.time() - (30 * 86400)
    _clean_dir(dirs, watermark)
