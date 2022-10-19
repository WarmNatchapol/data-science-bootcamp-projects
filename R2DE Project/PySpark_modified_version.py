# import modules
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.functions import col

# def function create spark session
def create_spark_session():
    spark = SparkSession.builder \
        .master("yarn") \
        .getOrCreate()
    return spark

 # def function read, merge, and save data
def merge_data(spark, input_data1, input_data2, output_data):
    # read data from gcs
    conversion_df = spark.read.csv(input_data1, header=True, inferSchema=True)
    audible_df = spark.read.csv(input_data2, header=True, inferSchema=True)

    # change data type
    conversion_df = conversion_df.withColumn("date", f.to_date(conversion_df.date, "yyyy-MM-dd"))
    audible_df = audible_df.withColumn("timestamp", f.to_timestamp(audible_df.timestamp, "M/d/yyyy H:mm"))

    # create new column
    audible_df = audible_df.withColumn("date", f.to_date(audible_df.timestamp))

    # join 2 dataframe
    output_df = audible_df.join(conversion_df, on = ["date"], how = "left")

    # delete $ and change to double data type
    udf = UserDefinedFunction(lambda x: x.replace("$",""))
    output_df = output_df.withColumn("Price", udf(col("Price")).cast("double"))

    # create new column
    output_df = output_df.withColumn("THBPrice", output_df.Price * output_df.conversion_rate)

    # drop unused column
    output_df = output_df.drop("date")

    # save to csv
    output_df.coalesce(1).write.csv(output_data, header = True)


# def main function and call main function
def main():
    spark = create_spark_session()
    input_data1 = "gs://asia-east2-modified-version-eaa4cbd5-bucket/data/conversion.csv"
    input_data2 = "gs://asia-east2-modified-version-eaa4cbd5-bucket/data/audible_data.csv"
    output_data = "gs://asia-east2-modified-version-eaa4cbd5-bucket/data/output.csv"
    merge_data(spark, input_data1, input_data2, output_data)
if __name__ == "__main__":
    main()
