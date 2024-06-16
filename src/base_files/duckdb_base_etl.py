import polars as pl

from src.base_files.duckdb_hook import DuckDBHook

class DuckDBBaseETL():

    def __init__(self, table_name:str, primary_key:list, load_transform_query:str):

        self.hook = DuckDBHook()
        self.table_name = table_name
        self.primary_key = primary_key
        self.load_transform_query = load_transform_query

    def execute(self):
        print(f"Starting {self.table_name} ETL process...")

        df = self.load_transform(self.load_transform_query)
        self.upsert_table(df)

    def load_transform(self, load_transform_query:str) -> pl.DataFrame:
        print("Loading and transforming data...")

        print(load_transform_query)

        df = self.hook.sql_to_dataframe(load_transform_query)

        print(df)

        return df

    def upsert_table(self, transformed_data:pl.DataFrame):
        print("Merging transformed data into target table...")

        update_columns = []

        for target_column in transformed_data.columns:
            if target_column not in self.primary_key:
                join = f'{target_column} = EXCLUDED.{target_column}'
                update_columns.append(join)

        update_columns = '\nAND\n'.join(update_columns)

        primary_columns = []

        for pk_columns in self.primary_key:
            primary_columns.append(f'{pk_columns} = EXCLUDED.{pk_columns}')

        primary_columns = '\nAND\n'.join(primary_columns)

        query = f"""
        INSERT INTO 
            {self.table_name}
        BY NAME 
            (SELECT * FROM transformed_data)
        ON CONFLICT DO UPDATE SET {update_columns} WHERE {primary_columns} AND {self.primary_key[0]} > EXCLUDED.{self.primary_key[0]}
        ;
        """

        print(query)

        self.hook.sql(query)