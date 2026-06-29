from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
from pyspark.sql import SparkSession
def main():
    HDFS_BASE = get_hdfs_base()
    spark = SparkSession.builder \
    .appName("WriteToExcel") \
    .config("spark.jars.packages", "com.crealytics:spark-excel_2.13-3.5.1_0.20.4") \
    .getOrCreate()
    #spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
    data = [
        (1, "Alice", 28),
        (2, "Bob", 35),
        (3, "Charlie", 42)
    ]


    # Define column names
    columns = ["id", "name", "age"]
    output_path="/app/Scripts/"
    # Create the DataFrame
    df = spark.createDataFrame(data, columns)
    df.show()
    df.write \
    .format("com.crealytics.spark.excel") \
    .option("dataAddress", "'Sheet1'!A1") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(output_path)



if __name__ == "__main__":
    main()    
