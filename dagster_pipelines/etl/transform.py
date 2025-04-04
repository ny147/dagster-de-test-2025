import pandas as pd

# 2.2.1 Pivot data in the "KPI_FY.xlsm" file
def pivot_data(df_ex) -> pd.DataFrame:
    df_pivot = df_ex.melt(id_vars=['Fiscal_Year', 'Center_ID', 'Kpi Number', 'Kpi_Name', 'Unit','Plan_Total','Actual_Total'], var_name='Amount_Name', value_name='Amount')
    df_pivot["Amount_Type"] = df_pivot.Amount_Name.apply(lambda x: x.split("_")[0])
    return df_pivot