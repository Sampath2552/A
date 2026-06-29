from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
from pyspark.sql.functions import col , sum as spark_sum
CIBA_CGL= "1106505001"
def main():
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
   
    output_path = "/reports/31-03-2026/ciba_details_report/ciba_details_report.xlsx"
    path_to_read="hdfs://10.177.103.199:8022/druid-data-lake/31-03-2026/GLIF"
    base_df = spark.read.format("delta").load(path_to_read).filter(col("CGL") == CIBA_CGL)
    base_df.show()
    unbalanced_journals = base_df.groupBy("JOURNAL_NUMBER", "currency", "CGL") \
       .agg(spark_sum("amount").alias("total_amount")) \
       .filter(col("total_amount") != 0) \
       .select("JOURNAL_NUMBER")
    final_df = base_df.join(unbalanced_journals, on="JOURNAL_NUMBER", how="left_semi")
    print(f"Final Count :{final_df.count()}")
    final_df.write \
    .format("com.crealytics.spark.excel") \
    .option("dataAddress", "'CIBA'!A1") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(output_path)    

    
    
    
    
    
    
if __name__ == "__main__":
    main()    
