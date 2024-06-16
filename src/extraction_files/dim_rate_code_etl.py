from base_files.duckdb_base_etl import DuckDBBaseETL

class DimRateCodeETL(DuckDBBaseETL):

    def __init__(self):

        table_name = 'dim_rate_code'
        primary_key = ['rate_code_id']
        load_transform_query = f"""
                                select distinct
                                    RateCodeID as vendor_id,
                                    case
                                        when RateCodeID = 1 then 'Standard Rate'
                                        when RateCodeID = 2 then 'JFK'
                                        when RateCodeID = 3 then 'Newark'
                                        when RateCodeID = 4 then 'Nassau or Westchester'
                                        when RateCodeID = 5 then 'Negotiated fare'
                                        when RateCodeID = 6 then 'Group ride'
                                        else null
                                    end as rate_code_description
                                from
                                    'yellow_tripdata_files/*.parquet'
                                """
        
        super().__init__(table_name, primary_key, load_transform_query)
