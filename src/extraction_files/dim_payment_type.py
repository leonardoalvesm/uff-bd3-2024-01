from src.base_files.duckdb_base_etl import DuckDBBaseETL

class DimPaymentTypeETL(DuckDBBaseETL):

    def __init__(self):

        table_name = 'dim_payment_type'
        primary_key = ['payment_type_id']
        load_transform_query = f"""
                                select distinct
                                    Payment_type as payment_type_id,
                                    case
                                        when Payment_type = 1 then 'Credit card'
                                        when Payment_type = 2 then 'Cash'
                                        when Payment_type = 3 then 'No charge'
                                        when Payment_type = 4 then 'Dispute'
                                        when Payment_type = 5 then 'Unknown'
                                        when Payment_type = 6 then 'Voided trip'
                                        else null
                                    end as payment_type_description
                                from
                                    'yellow_tripdata_files/*.parquet'
                                """
        
        super().__init__(table_name, primary_key, load_transform_query)
