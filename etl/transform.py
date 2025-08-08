import pandas as pd

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    df['cusname']=df['cusname'].str.capitalize()
    df['email']=df['email'].str.lower()
    df['dob']=pd.to_datetime(df['dob'],format='%m/%d/%y').dt.strftime('%d/%m/%y')
    df['current_city']=df['current_city'].fillna('NA')
    df['GRADE']=df['salary'].apply(lambda x:1 if x>30000 else 2)

    return df

