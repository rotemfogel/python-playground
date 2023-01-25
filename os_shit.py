import os
import time

from typing import List


def _get_logs_dir() -> str:
    return "/data/rotem/Downloads/installs"


def _list_dirs(path: str) -> str:
    prefix = f"{path}/" if not path.endswith("/") else path
    paths = [f.path for f in os.scandir(prefix) if f.is_dir()]
    # return comma delimited string to avoid parsing a list later
    return ",".join(paths)


def _clean_dir(paths: str, upper_bound: int) -> None:
    if paths:
        for root in paths.split(","):
            if os.stat(root).st_mtime < upper_bound:
                print(f"about to delete [{root}]")
            else:
                _clean_dir(_list_dirs(root), upper_bound)


if __name__ == "__main__":
    logs_dir = _get_logs_dir()
    dirs = _list_dirs(logs_dir)
    watermark = time.time() - (30 * 86400)
    _clean_dir(dirs, watermark)
