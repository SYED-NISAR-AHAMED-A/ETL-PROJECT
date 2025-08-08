import pymysql

def load_data(df,conn_details:dict,table_name='customer_data'):
    print("Loading the data")
    conn=pymysql.connect(
        host=conn_details['host'],
        user=conn_details['user'],
        password=conn_details['password'],
        database=conn_details['database']
    )

    cursor=conn.cursor()

    #drop the table
    cursor.execute(f"drop table if exists {table_name} ")

    #create table name
    # Create table with TEXT fields
    cols = ', '.join(f"`{col}` TEXT" for col in df.columns)
    cursor.execute(f"CREATE TABLE {table_name} ({cols})")   

    # Insert each row
    for _, row in df.iterrows():
        placeholders = ', '.join(['%s'] * len(row))
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.execute(sql, tuple(row))    

    conn.commit()
    cursor.close()
    conn.close()

    print("data loaded successfully")




    