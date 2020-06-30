from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
from pyspark.sql.functions import udf
from pyspark.sql import functions as F
import json

def parse_json(array_str):
    json_obj = json.loads(array_str)
    for item in json_obj:
       yield item["a"], item["b"]

if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("compute_stats") \
        .getOrCreate()

    csv_schema = StructType( [StructField('InvoiceNo', StringType(), nullable=False),
                                         StructField('Customer_ID', StringType(), nullable=False),
                                         StructField('Date', StringType(), nullable=False),
                                         StructField('Planet', StringType(), nullable=False),
                                         StructField('Purchase',StringType(), nullable=False) ] )


    # json_schema = ArrayType(StructType( [ StructField('ItemNo', StringType(), nullable=False),
    #                                      StructField('UnitPrice', StringType(), nullable=False),
    #                                      StructField('Description', StringType(), nullable=False),
    #                                      StructField('Quantity', StringType(), nullable=False) ] ) )

    json_schema = StructType([
        StructField("My_array", ArrayType(StructType([StructField('ItemNo', StringType(), nullable=True),
                                                      StructField('UnitPrice', StringType(), nullable=True),
                                                      StructField('Description', StringType(), nullable=True),
                                                      StructField('Quantity', StringType(), nullable=True)]))
                    )
    ])

    input_path = sys.argv[1]
    print( "input path : ", input_path )

    raw_data = spark.sparkContext.textFile( "/Users/sreekanthreddykarakula/Downloads/data.csv" )

#    raw_data.filter()

    results = spark \
        .read \
        .schema(csv_schema) \
        .option("header", "true") \
        .csv("/Users/sreekanthreddykarakula/Downloads/data.csv")

    print( "schema", results.printSchema() )

    udf_parse_json = udf(lambda str: parse_json(str), json_schema)

    #purchases = results.select("Customer_ID", udf_parse_json(results.Purchase) )

    #results2 = results.withColumn("Purchase_json", F.split("Purchase", ",") )
    results2 = results.withColumn("Purchase_json", F.from_json("Purchase", json_schema))
    results2.show()

    # display final results
    # print( "results = ", results2.printSchema )

    spark.stop()