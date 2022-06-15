from typing import List


def foo(tables: List[str], excluded_tables: List[str]):
    if tables:
        exclude_tables = excluded_tables or []
        for table in tables:
            # skip temporary and excluded tables
            if table not in exclude_tables and not table.startswith('tmp_'):
                print(f'backing up table {table}')
        print(f'exported {len(tables)} tables')


if __name__ == '__main__':
    t = ['table_a', 'table_b', 'tmp_table_c']
    e = ['table_a']
    foo(t, e)
    foo([], [])
    foo(t, [])
    foo(t, None)
    foo(None, None)
