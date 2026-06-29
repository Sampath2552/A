from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
from pyspark.sql.functions import col , sum as spark_sum


CIBA_CGL= "1106505001"
TECHNICAL_SUSPENSE_CGL="1260505001"

HDFS_BASE = get_hdfs_base()


def generate_ciba_details_report(spark):
    output_path = f"{HDFS_BASE}/reports/2026-06-27/ciba_details/ciba_details_report.xlsx"
    #path_to_read=f"{HDFS_BASE}/druid-data-lake/2026/JUN/27/GLIF/GLIF_TXNS/"
    path_to_read=f"{HDFS_BASE}/druid-data-lake/31-03-2026/GLIF"
    
    journal_df = spark.read.format("delta").load(path_to_read).filter(col("CGL") == CIBA_CGL)
    journal_df.show()
    
    
    unbalanced_journals = journal_df.groupBy("JOURNAL_NUMBER", "CURRENCY") \
       .agg(spark_sum("AMOUNT").alias("total_amount")) \
       .filter(col("total_amount") != 0) \
       .select("JOURNAL_NUMBER") \
       .distinct()
    print(f"Unbalanced Journals Count:{unbalanced_journals.count()}")   
    
    
    unbalanced_ids_list = [row.JOURNAL_NUMBER for row in unbalanced_journals.collect()]
    
    if unbalanced_ids_list:
    
        broadcast_ids = spark.sparkContext.broadcast(unbalanced_ids_list)
        final_unbalanced_df = spark.read.format("delta").load(path_to_read) \
            .filter(col("JOURNAL_NUMBER").isin(broadcast_ids.value))
        final_unbalanced_df.show()
    else:
        final_unbalanced_df = spark.createDataFrame([], schema=journal_df.schema)
        print("No unbalanced journals found.")
    
    
    
#     base_df.createOrReplaceTempView("glif_records")
    
#     unbalanced_journals = spark.sql("""
#        SELECT *
#        FROM glif_records
#        WHERE JOURNAL_NUMBER IN (
#            SELECT JOURNAL_NUMBER
#            FROM glif_records
#            GROUP BY JOURNAL_NUMBER, CURRENCY
#            HAVING SUM(AMOUNT) != 0
#        )
#    """)
#     unbalanced_journals.show(50, truncate=False)
    
#     print(f"Count of unbalanced_journals :{unbalanced_journals.count()}")
       
       
    # final_df = journal_df.join(unbalanced_journals, on="JOURNAL_NUMBER", how="left_semi")
    print(f"Final Count :{final_unbalanced_df.count()}")
    final_unbalanced_df.limit(150).write \
    .format("com.crealytics.spark.excel") \
    .option("dataAddress", "'CIBA'!A1") \
    .option("header", "true") \
    .option("maxRowsInMemory", 5000) \
    .mode("overwrite") \
    .save(output_path)    


def generate_technical_suspense_report(spark):
    pass
    path_to_read=f"{HDFS_BASE}/druid-data-lake/31-03-2026/GLIF"
    #TO ADD: Filter to get BATCH ID which are not null
    base_df = spark.read.format("delta").load(path_to_read).filter(col("CGL") == TECHNICAL_SUSPENSE_CGL)
    
    
    unbalanced_batches = base_df.groupBy("BATCH_ID") \
        .agg(spark_sum("AMOUNT").alias("total_amount")) \
       .filter(col("total_amount") != 0) \
       .select("JOURNAL_NUMBER") \
       .distinct()
    
    unbalanced_batches_journals_list =    [row.JOURNAL_NUMBER for row in unbalanced_batches.collect()]
    broadcast_ids = spark.sparkContext.broadcast(unbalanced_batches_journals_list)
    unbalanced_batches_df =  spark.read.format("delta").load(path_to_read) \
            .filter(col("JOURNAL_NUMBER").isin(broadcast_ids.value))
    




def main():
    
    spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
    
    #generate_ciba_details_report(spark)
    generate_technical_suspense_report(spark)
    

if __name__ == "__main__":
    main()    
