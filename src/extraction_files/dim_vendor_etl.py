from src.base_files.duckdb_base_etl import DuckDBBaseETL

class DimVendorETL(DuckDBBaseETL):

    def __init__(self):

        table_name = 'dim_vendor'
        primary_key = ['vendor_id']
        load_transform_query = f"""
                                select distinct
                                    VendorID as vendor_id,
                                    case
                                        when VendorID = 1 then 'Creative Mobile Technologies, LLC'
                                        when VendorID = 2 then 'VeriFone Inc.'
                                        else null
                                    end as vendor_description
                                from
                                    'yellow_tripdata_files/*.parquet'
                                """
        
        super().__init__(table_name, primary_key, load_transform_query)

