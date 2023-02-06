from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("testing").master("local").enableHiveSupport().getOrCreate()
print("Debit details historical data computation started")

df_txtp_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl","localhost:2181").load()
df_txtp_tmp.createOrReplaceTempView("debit_tmp")

df_result=spark.sql("""select
                    D.c_number as CardNumber,
                    D.tx_id as TransactionId,
                    D.tx_date as transactionDate,
                    D.amt_spend as amountspend,
                    D.category as category,
                    D.d_status as debitstatus
                    from debit_tmp D
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.debit_details_staging")

spark.catalog.dropTempView("debitt_tmp")

#give alias
res=df_txtp_tmp.selectExpr("C_NUMBER AS CardNumber","TX_ID as TransactionId","TX_DATE as transactionDate","AMT_SPEND as amountspend","CATEGORY as category","D_STATUS as debitstatus")
res.show(5)

#write data in hive
res.write.mode('overwrite').saveAsTable("prod.debit_details_staging")
print("debit details data written successfully in hive staging")

spark.stop()
