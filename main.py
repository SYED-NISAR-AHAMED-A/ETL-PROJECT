from fastapi import  FastAPI
from pydantic import BaseModel
from typing import Dict
import pandas as pd
from etl.extract import extract_csv
from etl.transform import transform_data
from etl.load import load_data


#pydantic models    
class Extract_config(BaseModel):
    path:str
class Load_config(BaseModel):
    conn:Dict[str,str]
class ETLRequest(BaseModel):
    ext:Extract_config
    load:Load_config

app=FastAPI()

@app.post('/ETL_RUN')
def run_etL(config:ETLRequest):

    try:
        df=extract_csv(config.ext.path)
        #transform
        df=transform_data(df)
        #load
        load_data(df,config.load.conn)
        return {
            "status": "success",
            "rows_loaded": len(df)
        }
    except Exception as e:
        return{
            "status":"error",
            "message":str(e)
        }

