from __future__ import print_function
import pandas as pd
from sqlalchemy import create_engine
import sys
import json

if __name__ == "__main__":
    data = pd.read_csv("/Users/sreekanthreddykarakula/Downloads/data.csv")
    #data.count()

    data.dtypes
    print("input path : ", data.dtypes)

    data.map( )

    ##change types of dataframe to respective ones

    purchase_data = data["Purchase"]

    purchase_data.map(parse_json)

    sqlEngine = create_engine('mysql+pymysql://root:karakula@127.0.0.1/ANALYTICS', pool_recycle=3600)
    dbConnection = sqlEngine.connect()
    try:

     #frame = data.to_sql("Invoice", dbConnection, if_exists='fail');

    except ValueError as vx:
 
        print(vx)

    except Exception as ex:

        print(ex)

    else:

        print("Table %s created successfully." % "Invoice");

    finally:

        dbConnection.close()


def parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj:
       yield (item["a"], item["b"])