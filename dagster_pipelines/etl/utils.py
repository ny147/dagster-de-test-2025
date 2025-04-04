# 2.5 data validation 
def check_column_type(df,expect_column_type):
    for i in df.columns:
        if df[i].dtype != expect_column_type[i]:
            raise TypeError(f"Column '{i}' expected {expect_column_type[i]}, but got {df[i].dtype}")
    return True