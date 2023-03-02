import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy.pool as pool
import psycopg2


data = "path/to/data.csv"
dtf = pd.read_csv(data, sep=",")
del dtf["Unnamed: 0"]

url = "postgresql://"+user+":"+password+"@"+host+":"+port+"/"+database
engine = create_engine(url=url)
dtf.to_sql("name_of_the_tb", engine, if_exists="replace")
