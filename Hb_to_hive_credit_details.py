from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("testing").master("local").enableHiveSupport().getOrCreate()

print("credit details historical data computation started")

df_pd_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_Paid_HB").option("zkUrl","localhost:2181").load()

df_tt_tmp=spark.read.format("org.apache.phoenix.spark").option("table","TX_TYPE_HB").option("zkUrl","localhost:2181").load()

df_pd_tmp.createOrReplaceTempView("credit_tmp")
df_tt_tmp.createOrReplaceTempView("txtype_tmp")

df_result=spark.sql("""select
                    P.c_number as CardNumber,
                    P.tx_id as TransactionId,
                    P.tx_date as transactionDate,
                    P.amt_paid as amountpaid,
                    P.c_status as cardstatus,
                    tt.tx_type_desc as transaction_type_desc
                    from credit_tmp P
                    LEFT JOIN txtype_tmp tt on p.tx_type_id=tt.tx_type_id
""")

#df_result.show(10)
#df_result.write.mode('overwrite').saveAsTable("prod.credit_details_staging")

spark.catalog.dropTempView("credit_tmp")
spark.catalog.dropTempView("txtype_tmp")

#DSL
test=df_pd_tmp.join(df_tt_tmp,"tx_type_id", how="left").drop("TX_TYPE_ID")

#give alias
res=test.selectExpr("C_NUMBER AS CardNumber","TX_ID as TransactionId","TX_DATE as transactionDate","AMT_PAID as amountpaid","C_STATUS as cardstatus","TX_TYPE_DESC as transaction_type_desc")

res.show(5)

#write data in hive

res.write.mode('overwrite').saveAsTable("prod.credit_details_staging")

print("credit details data written successfully in hive staging")

spark.stop()
