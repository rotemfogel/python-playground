import re
from typing import List

from pendulum import Pendulum, datetime

tables = [f'tmp_table_{Pendulum.now().subtract(days=i).format("%Y%m%d_%H")}' for i in [0, 1, 2, 3]]
filtered_tables = tables[1:]


class FilterTables:

    def __init__(self,
                 database: str,
                 tables_to_remove: List[str] = None,
                 filter_fn=lambda tables_to_remove: tables_to_remove):
        super(FilterTables, self).__init__()
        self.database = database
        self.filter_fn = filter_fn
        self.tables_to_remove = tables_to_remove if tables_to_remove else []

    def execute(self) -> List[str]:
        # if no tables were provided, pull the table list
        if not self.tables_to_remove:
            self.tables_to_remove = tables
        # apply the filter function (won't do anything, if not declared)
        return self.filter_fn(self.tables_to_remove)


def __match(today: Pendulum, table: str) -> (str, Pendulum):
    match_name = re.search(r'^tmp_.*\d{4}\d{2}\d{2}', table)
    match_date = re.search(r'\d{4}\d{2}\d{2}', table)
    return table, datetime.strptime(match_date.group(), '%Y%m%d') if match_date and match_name else today


def __filter_tmp_tables(table_list: List[str]) -> List[str]:
    execution_date: Pendulum = Pendulum.now()
    upper_bounds = execution_date.subtract(days=1)
    to_delete = map(lambda x: __match(execution_date, x), table_list)
    return [x[0] for x in list(filter(lambda x: x[1] < upper_bounds, to_delete))]


if __name__ == "__main__":
    f = FilterTables(database='db')
    f_tables = f.execute()
    print(f_tables)
    assert f_tables == tables
    f = FilterTables(database='db', tables_to_remove=tables)
    f_tables = f.execute()
    print(f_tables)
    assert f_tables == tables
    f = FilterTables(database='db', tables_to_remove=tables, filter_fn=__filter_tmp_tables)
    f_tables = f.execute()
    print(f_tables)
    assert f_tables == filtered_tables
    f = FilterTables(database='db', filter_fn=__filter_tmp_tables)
    f_tables = f.execute()
    print(f_tables)
    assert f_tables == filtered_tables
