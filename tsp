from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base
import common.constants as constants
from common.dateUtil import get_etl_date
from pyspark.sql.functions import col
CIBA_CGL= "1106505001"
def main():
    HDFS_BASE = get_hdfs_base()
    spark = create_spark_session("Technical_Suspense_Details",HDFS_BASE)
   

    path_to_read="hdfs://10.177.103.199:8022/druid-data-lake/31-03-2026/GLIF"
    base_df = spark.read.format("delta").load(path_to_read).filter(col("CGL") == CIBA_CGL)
    base_df.show()

if __name__ == "__main__":
    main()    
