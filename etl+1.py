#ETL+1 pipeline


import sys
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, concat
from pyspark.sql.types import DateType, DecimalType
from common.log_etl_modified import log_etl
from common.logger import setup_logger
from common.properties import get_oracle_properties
from common.dateUtil import get_etl_date
from common.fileUtil import paths_for_read
from common.processRun import get_run_id, get_process_run_id
from common.createSpark import create_spark_session
from common.fileUtil import get_hdfs_base

from common.read_write_oracle import read_oracle, write_oracle
from common.constants import Process
# ==========================================================
# Spark & ETL setup
# ==========================================================
HDFS_BASE = get_hdfs_base()
spark = create_spark_session("GLIF_Transfer[ETL+1]_PIPELINE", HDFS_BASE)

# db_date_str = "28-FEB-26"
# today = datetime.strptime(db_date_str, "%d-%b-%y")
# etl_plus_1_date = today + timedelta(days=1)
# posting_date_str = today.strftime("%Y-%m-%d")        # "2025-11-21"
# etl_plus_1_str = etl_plus_1_date.strftime("%Y-%m-%d") # "2025-11-22"

today = get_etl_date(spark)
etl_plus_1_date = today + timedelta(days=1)

posting_date_str = today.strftime("%Y-%m-%d")
etl_plus_1_str = etl_plus_1_date.strftime("%Y-%m-%d")

PROCESS_ID = Process.ETL_PLUS_ONE
RUN_ID = get_run_id(spark, posting_date_str, PROCESS_ID)
logger = setup_logger("GLIF_DATA_TRANSFER", RUN_ID, posting_date_str)

process_run_id = (
    sys.argv[1] if len(sys.argv) > 1
    else get_process_run_id(spark, PROCESS_ID, posting_date_str)
)

logger.info(f"=== GLIF DATA TRANSFER STARTED FOR {posting_date_str} ===")
log_etl(spark, process_run_id, "gl_data_transfer_started", 1, "JOB STARTED")

hdfs_paths = paths_for_read("glif",today,posting_date_str)
GL_DATALAKE_PATH = hdfs_paths["GL_DATALAKE_PATH"]
# ==========================================================
# step 1: Reading closing balance from GLIF DELTA LAKE
# ==========================================================
# logger.info(f"=== step 1: reading closing balance for {posting_date_str} ===")
# log_etl(spark, process_run_id, "reading_closing_balance", 1, "Reading from GLIF Delta Lake")

# path_config = paths_for_read("glif", today, posting_date_str)
# gl_delta_path = path_config["GL_DATALAKE_PATH"]

# try:
#     closing_balance_df = spark.read.format("delta").load(gl_delta_path) \
#         .filter(col("BALANCE_DATE") == lit(posting_date_str)) \
#         .select("CGL", "CURRENCY", "BRANCH_CODE", "BALANCE", "INR_BALANCE")
#     logger.info(f"=== closing balance read successfully.")
#     log_etl(spark, process_run_id, "reading_closing_balance", 2, f"Read records from Delta")

# except Exception as e:
#     logger.error(f"error reading closing balance: {e}")
#     log_etl(spark, process_run_id, "reading_closing_balance", 3, "Error reading from Delta")
#     spark.stop()
#     exit(1)


# ==========================================================
# STEP 1: Read closing balance
# ==========================================================
log_etl(spark, process_run_id, "reading_closing_balance", 1, "Reading from GL BALANCE")
gl_balance_query = f"""
(
    SELECT CGL, CURRENCY, BRANCH_CODE, BALANCE, INR_BALANCE
    FROM GL_BALANCE
    WHERE BALANCE_DATE = TO_DATE('{posting_date_str}', 'YYYY-MM-DD')
) T1
"""
# gl_balance_query = f"""
# (
#     SELECT CGL, CURRENCY, BRANCH_CODE, BALANCE, INR_BALANCE
#     FROM GL_BALANCE
#     WHERE BALANCE_DATE = (SELECT MAX(BALANCE_DATE) FROM GL_BALANCE)
# ) T1
# """
closing_balance_df = read_oracle(spark, gl_balance_query)
log_etl(spark, process_run_id, "reading_closing_balance", 2, "Reading from GL BALANCE")

# ==========================================================
# STEP 2: Normal day fast path
# ==========================================================
is_year_end = (today.month == 3 and today.day == 31)
opening_balance_df = spark.createDataFrame([], closing_balance_df.schema)
if not is_year_end:

    logger.info("=== Normal day processing ===")
    log_etl(spark, process_run_id, "creating_opening_bal", 1, "Creating opening balance")
    opening_balance_df = closing_balance_df.withColumn(
        "BALANCE_DATE",
        lit(etl_plus_1_str).cast(DateType())
    )
    log_etl(spark, process_run_id, "creating_opening_bal", 2, "Opening balance created")

# ==========================================================
# STEP 3: Year-end logic (only on March 31)
# ==========================================================
else:
    logger.info("=== Year-end detected ===")
    cal_active_df = read_oracle(spark, "(SELECT 1 FROM CALENDER_CONFIG WHERE ACTIVE_FLAG = 1) T1")

    if not cal_active_df.isEmpty():
        logger.info("=== Active financial year found ===")
        log_etl(spark, process_run_id, "year_end_rule", 1, "Active financial year found")
        log_etl(spark, process_run_id, "creating_opening_bal", 1, "Creating opening balance")

        # Base Data: Setting the new date
        base_df = closing_balance_df.withColumn("BALANCE_DATE", lit(etl_plus_1_str).cast("date"))
    
        # 1. Load Masters for Filtering
        cgl_master_df = read_oracle(spark, "(SELECT CGL_NUMBER FROM CGL_MASTER WHERE BAL_FWD = 1) T1")
        circle_master_df = read_oracle(spark, "(SELECT DISTINCT BRANCH_CODE FROM CIRCLE_MASTER WHERE BRANCH_CODE IS NOT NULL) T1")

        # 2. Only keeping CGLs where BAL_FWD = 1
        # Doing this first to ensure no other logic runs on excluded CGLs
        fwd_base_df = base_df.join(F.broadcast(cgl_master_df), 
                                    F.trim(F.col("CGL")) == F.trim(F.col("CGL_NUMBER")), 
                                    "inner").select(base_df.columns)

        # ---  CGL 7 & 8 RULE ---
        rule_78_base = base_df.filter(F.col("CGL").rlike("^[78]"))
        original_78 = rule_78_base.filter(F.trim(F.col("CURRENCY")) != "INR").distinct()

        # Creating the INR Offset rows
        offset_78 = original_78.withColumn("CURRENCY", F.lit("INR")) \
                                .withColumn("BALANCE", F.negate(F.col("INR_BALANCE"))) \
                                .withColumn("INR_BALANCE", F.negate(F.col("INR_BALANCE")))

        # Union (1 Foreign + 1 INR = 2 rows)
        df_78_final = original_78.unionByName(offset_78)

        # --- CGL 2110505001 rule (Circle Branches) ---
        # Now automatically respects BAL_FWD=1 because it uses fwd_base_df
        df_circle_final = fwd_base_df.filter(F.col("CGL") == "2110505001") \
                                        .join(F.broadcast(circle_master_df), "BRANCH_CODE", "inner")

        # --- General rule ---
        # Excluding the 7/8 and Circle CGLs
        df_general_final = fwd_base_df.filter(
            (~(F.col("CGL").cast("string").rlike("^[78]"))) & 
            (F.col("CGL").cast("string") != "2110505001")
        )

        # --- Final union ---
        common_cols = ["CGL", "CURRENCY", "BRANCH_CODE", "BALANCE_DATE", "BALANCE", "INR_BALANCE"]
            
        opening_balance_df = df_78_final.select(*common_cols) \
                                .unionByName(df_circle_final.select(*common_cols)) \
                                .unionByName(df_general_final.select(*common_cols))

        # opening_balance_df.show(200, False)
        log_etl(spark, process_run_id, "creating_opening_bal", 2, "Opening balance created")
    else:
        logger.info("=== No active financial year. Skipping ===")
        log_etl(spark, process_run_id, "year_end_rule", 1, "Skipped: Inactive Year")

        
# ==========================================================
# STEP 4: Writing to Delta
# ==========================================================
logger.info(f"=== Writing opening balance to delta for {etl_plus_1_str} ===")
log_etl(spark, process_run_id, "writing_opening_balance_delta", 1, "Writing to Delta Lake")

delta_final_df = opening_balance_df.select(
    concat(col("BRANCH_CODE"), col("CURRENCY"), col("CGL")).alias("GLCC"),
    col("BALANCE").cast(DecimalType(25, 4)).alias("CLOSING_BALANCE"),
    col("BALANCE_DATE")
)

delta_final_df.write.format("delta").mode("overwrite") \
    .option("replaceWhere", f"BALANCE_DATE = '{etl_plus_1_str}'") \
    .save(GL_DATALAKE_PATH)

logger.info("=== Delta write completed ===")
log_etl(spark, process_run_id, "writing_opening_balance_delta", 2, "Delta write successful")


# ==========================================================
# STEP 5: Writing to Oracle
# ==========================================================
logger.info("=== Writing opening balance to oracle balance table ===")
log_etl(spark, process_run_id, "writing_opening_balance_oracle", 1, "Writing to Oracle")

write_oracle(
    opening_balance_df.select(
        col("CGL"),
        col("CURRENCY"),
        col("BRANCH_CODE"),
        col("BALANCE_DATE"),
        col("BALANCE").cast(DecimalType(25, 4)),
        col("INR_BALANCE").cast(DecimalType(25, 4))
    ),
    "GL_BALANCE"
)

logger.info("=== Oracle write completed ===")
log_etl(spark, process_run_id, "writing_opening_balance_oracle", 2, "Oracle write successful")


# ==========================================================
# STEP 6: Updating FINCORE_DATE_DEV 
# ==========================================================
try:
    logger.info(f"=== Updating FINCORE_DATE_DEV to {etl_plus_1_str} ===")
    log_etl(spark, process_run_id, "fincore_date_update", 1, "Updating user's date")

    oracle_props = get_oracle_properties()
    url = oracle_props['url']
    user = oracle_props['user']
    pwd = oracle_props['password']

    conn = spark._jvm.java.sql.DriverManager.getConnection(url, user, pwd)
    conn.setAutoCommit(False)

    stmt = conn.createStatement()

    update_sql = f"""
        UPDATE FINCORE_DATE_DEV
        SET USERS_DATE = TO_DATE('{etl_plus_1_str}', 'YYYY-MM-DD'),
            LAST_UPDATED_DATE = SYSTIMESTAMP
        WHERE ROWNUM = 1
    """

    stmt.executeUpdate(update_sql)
    conn.commit()

    stmt.close()
    conn.close()

    logger.info("=== FINCORE_DATE updated successfully ===")
    log_etl(spark, process_run_id, "fincore_date_update", 2, "User's date advanced")

except Exception as e:
    logger.error(f"Failed to update FINCORE_DATE: {e}")
    log_etl(spark, process_run_id, "job_failed", 3, f"Fatal Error: {str(e)[:200]}")
    raise e

logger.info("=== GLIF DATA TRANSFER COMPLETED SUCCESSFULLY ===")
log_etl(spark, process_run_id, "gl_data_transfer_started", 2, "JOB COMPLETED")

spark.stop()
