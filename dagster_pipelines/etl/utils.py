# 2.5 data validation 
def check_column_type(df,expect_column_type):
    for i in df.columns:
        if df[i].dtype != expect_column_type[i]:
            raise TypeError(f"Column '{i}' expected {expect_column_type[i]}, but got {df[i].dtype}")
    return True

def convert_type(df,column_type):
    for i in df.columns :
        df[i] = df[i].astype(column_type[i])
    return df

def cleanup_column_Actual_Total(df):
    df["Actual_Total"] = df["Actual_Total"].replace('No',None)
    return df