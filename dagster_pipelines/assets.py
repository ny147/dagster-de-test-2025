import dagster as dg
from dagster_pipelines.etl.extract import read_excel, read_csv
from dagster_pipelines.etl.transform import pivot_data
from dagster_pipelines.etl.load import load_to_duckdb
from dagster_pipelines.etl.utils import check_column_type,cleanup_column_Actual_Total,convert_type
from dagster import get_dagster_logger
import sys
import duckdb

## Extra point uncomment 1 , 2 ,3 and comment  2.3.1.1 
## Extra point 1
# @dg.asset(compute_kind="duckdb", group_name="plan")
# def kpi_fy_data_convert_dataType(context: dg.AssetExecutionContext):
#     column_type = {

#     'Fiscal_Year' : 'int64', 'Center_ID' :'string', 'Kpi Number' :'string', 'Kpi_Name':'string', 'Unit':'string',
#        'Plan_Total' : 'float64', 'Plan_Q1': 'float64', 'Plan_Q2': 'float64', 'Plan_Q3': 'float64', 'Plan_Q4': 'float64',
#        'Actual_Total' : 'float64', 'Actual_Q1': 'float64', 'Actual_Q2': 'float64', 'Actual_Q3': 'float64', 'Actual_Q4': 'float64'
#     }

#     df_kpi = read_excel()
#     df_clean = cleanup_column_Actual_Total(df_kpi)
#     df_convert = convert_type(df_clean,column_type)

#     #write data into new file
#     df_convert.to_csv('dagster_pipelines/data/KPI_FY_NewType.csv', encoding='utf-8', index=False)
    


## Extra point 2
# @dg.asset(compute_kind="duckdb", group_name="plan",deps=["kpi_fy_data_convert_dataType"])
# def kpi_fy_data_validation(context: dg.AssetExecutionContext):
#     column_type = {

#     'Fiscal_Year' : 'int64', 'Center_ID' :'object', 'Kpi Number' :'object', 'Kpi_Name':'object', 'Unit':'object',
#        'Plan_Total' : 'float64', 'Plan_Q1': 'float64', 'Plan_Q2': 'float64', 'Plan_Q3': 'float64', 'Plan_Q4': 'float64',
#        'Actual_Total' : 'float64', 'Actual_Q1': 'float64', 'Actual_Q2': 'float64', 'Actual_Q3': 'float64', 'Actual_Q4': 'float64'
#     }

#     df_kpi = read_csv("KPI_FY_NewType")
#     check_column_type(df_kpi,column_type)
#     pass

## Extra point 3
# @dg.asset(compute_kind="duckdb", group_name="plan",deps=["kpi_fy_data_validation"])
# def kpi_fy(context: dg.AssetExecutionContext):
#     df_kpi = read_csv("KPI_FY_NewType")
#     df_pivot = pivot_data(df_kpi)
#     load_to_duckdb(df_pivot,"KPI_FY")
    
#     pass


# 2.3.1.1 Load pivoted KPI_FY.xlsm into KPI_FY
@dg.asset(compute_kind="duckdb", group_name="plan")
def kpi_fy(context: dg.AssetExecutionContext):
    df_kpi = read_excel()
    df_pivot = pivot_data(df_kpi)
    load_to_duckdb(df_pivot,"KPI_FY")
    
    pass

# 2.3.1.2 Load M_Center.csv into M_Center
@dg.asset(compute_kind="duckdb", group_name="plan")
def m_center(context: dg.AssetExecutionContext):
    df_m_center = read_csv("M_Center")
    load_to_duckdb(df_m_center,"M_Center")
    pass

# 2.3.2 Create asset kpi_fy_final_asset()
@dg.asset(compute_kind="duckdb", group_name="plan",deps=["m_center","kpi_fy"])
def kpi_fy_final_asset(context: dg.AssetExecutionContext):
    logger = get_dagster_logger()
    try:
        with duckdb.connect("/opt/dagster/app/dagster_pipelines/db/plan.db") as con:
            logger.info("Connected to DuckDB successfully.")
            
            query = "SELECT * ,CURRENT_TIMESTAMP as updated_at FROM plan.plan.KPI_FY FY inner join plan.plan.M_Center mc On mc.Center_ID=FY.Center_ID"
            result_df = con.execute(query).fetchdf()
            
            load_to_duckdb(result_df,"KPI_FY_Final")

    except Exception as e:
        logger.error(f"Error loading data into DuckDB: {e}")
        raise
    