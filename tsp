def generate_technical_suspense_report(spark,etl_date,process_run_id):
    
    input_path,output_path=path_builder(etl_date,CIBA)
    print(f"Reading from path : {input_path}")
    base_df = spark.read.format("delta").load(input_path).filter(col("CGL") == TECHNICAL_SUSPENSE_CGL)
    base_df.cache()
    batch_df = base_df.filter(col(BATCH_ID_COLUMN).isNotNull())
    journal_df = base_df.filter(col(BATCH_ID_COLUMN).isNull())
    
    unbalanced_batches = batch_df.groupBy(BATCH_ID_COLUMN,BRANCH_COLUMN,CURRENCY) \
        .agg(spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)) \
       .filter(col(TOTAL_AMOUNT) != 0) \
       .select(BATCH_ID_COLUMN) \
       .distinct()
    unbalanced_batches_ids_list = [row.JOURNAL_NUMBER for row in unbalanced_batches.collect()]
    if unbalanced_batches_ids_list:
        broadcast_batch_ids = spark.sparkContext.broadcast(unbalanced_batches_ids_list)
        unbalanced_batches_df = spark.read.format("delta").load(input_path) \
                .filter(col(BATCH_ID_COLUMN).isin(broadcast_batch_ids.value)) 
    else:
        unbalanced_batches_df = spark.createDataFrame([], schema=base_df.schema)
        print("No Non-Zero Batches found.")
        #log_etl(spark, process_run_id, "journal_extract",2,"Completed Extracting Non-Zero journals. No Non-Zero Journals found.") 
        

        
    unbalanced_journals = journal_df.groupBy(JOURNAL_NUMBER,BRANCH_COLUMN,CURRENCY) \
        .agg(spark_sum(AMOUNT_COLUMN).alias(TOTAL_AMOUNT)) \
       .filter(col(TOTAL_AMOUNT) != 0) \
       .select(BATCH_ID_COLUMN) \
       .distinct()
    
    unbalanced_journal_ids_list = [row.JOURNAL_NUMBER for row in unbalanced_journals.collect()]
    
    if unbalanced_journal_ids_list:
        broadcast_journal_ids = spark.sparkContext.broadcast(unbalanced_journal_ids_list)
        unbalanced_journals_df = spark.read.format("delta").load(input_path) \
            .filter(col(JOURNAL_NUMBER).isin(broadcast_journal_ids.value)) 
        
    else:
        unbalanced_journals_df = spark.createDataFrame([], schema=journal_df.schema)
        print("No Journals Found for the date")
    
    
    
    unbalanced_batches_df.write \
        .format("com.crealytics.spark.excel") \
        .option("dataAddress", "'BATCH'!A1") \
        .option("header", "true") \
        .option("maxRowsInMemory", 5000) \
        .mode("overwrite") \
        .save(output_path) 
        
    unbalanced_journals_df.write \
        .format("com.crealytics.spark.excel") \
        .option("dataAddress", "'JOURNAL'!A1") \
        .option("header", "true") \
        .option("maxRowsInMemory", 5000) \
        .mode("overwrite") \
        .save(output_path) 
