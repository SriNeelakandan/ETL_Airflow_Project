import pymysql
import pandas as pd
from datetime import datetime
import os
HOST ='localhost'
USER_NAME= 'root'
PASSWORD= 'Neels@trichy634'
DATABASE= 'sample'

config ={
    'host': HOST,
    'user': USER_NAME,
    'password':PASSWORD,
    'database':DATABASE
}

def extract_data():
    connection =pymysql.connect(**config)
    query= "select * from sales"
    df=pd.read_sql(query,connection)
    connection.close()
    return (df)

def transform_data(y):
    df_transformed =y
    df_transformed.dropna(how='any',inplace=True)
    df_transformed["Total"] =    df_transformed["Unit_price"] * df_transformed["Quantity"]
    df_transformed["Amount_Paid"] = df_transformed["Total"] + (df_transformed["Total"] * 0.05)
    return (df_transformed)

def load_data(z):
    output_dir='/home/neels/output'
    os.makedirs(output_dir,exist_ok=True)
    timestamp= datetime.now().strftime('%d%m%Y_%H-%M-%S')
    file_name = os.path.join(output_dir, f"etl_output_{timestamp}.csv")
    z.to_csv(file_name,index=False)
    print(f"Data written to {file_name}")
    
def etl_process():
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)

etl_process()
