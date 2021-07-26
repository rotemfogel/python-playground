import re
from typing import List

from pendulum import Pendulum, datetime


class FilterTables:

    def __init__(self,
                 table_list: List[str],
                 filter_fn=lambda table_list: table_list):
        super(FilterTables, self).__init__()
        self.table_list = table_list
        self.filter_fn = filter_fn

    def execute(self) -> List[str]:
        return self.filter_fn(self.table_list)


def __match(today: Pendulum, table: str) -> (str, Pendulum):
    match = re.search(r'\d{4}\d{2}\d{2}', table)
    return table, datetime.strptime(match.group(), '%Y%m%d') if match else today


def __filter_tmp_tables(table_list: List[str]) -> List[str]:
    execution_date: Pendulum = Pendulum.now()
    upper_bounds = execution_date.subtract(days=1)
    to_delete = map(lambda x: __match(execution_date, x), table_list)
    return [x[0] for x in list(filter(lambda x: x[1] < upper_bounds, to_delete))]


if __name__ == "__main__":
    tables = [f'tmp_table_{Pendulum.now().subtract(days=i).format("%Y%m%d")}' for i in [0, 1, 2, 3]]
    f = FilterTables(table_list=tables)
    f_tables = f.execute()
    print(f_tables)
    assert f_tables == tables
    f = FilterTables(table_list=tables, filter_fn=__filter_tmp_tables)
    f_tables = f.execute()
    print(f_tables)
