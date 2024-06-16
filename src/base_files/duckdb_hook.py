import duckdb
import polars as pl


class DuckDBHook():

    def __init__(self, database = 'database/data_warehouse') -> None:

        self.cursor = duckdb.connect(database)

    def execute_dump_sql(self, path:str) -> None:

        file = open(path, 'r')
        file = file.read()
        
        for sql_clause in file.split(';'):
            sql_clause = sql_clause.replace(';','').strip()
            if sql_clause:
                print(f'Executing: {sql_clause}')
                self.cursor.sql(sql_clause)

    def sql(self, sql:str):

        return self.cursor.sql(sql)

    def execute(self, sql:str):

        return self.cursor.execute(sql)
    
    def sql_to_dataframe(self, sql:str):

        return self.cursor.sql(sql).pl()
