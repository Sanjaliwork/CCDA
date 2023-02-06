from pyspark.sql import *
from pyspark.sql import functions as F

spark = SparkSession.builder.master("local").appName("testing").enableHiveSupport().getOrCreate()
sc = spark.sparkContext

#df_cd_tmp = spark.sqlContext.read.format("phoenix").options(Map("table" -> "CARD",PhoenixDataSource.ZOOKEEPER_URL -> "phoenix-server:2181")).load
df_cd_tmp = spark.read.format("org.apache.phoenix.spark").option("table","CARD_HB").option("zkUrl", "localhost:2181").load()
df_ad_tmp = spark.read.format("org.apache.phoenix.spark").option("table","ADDRESS_HB").option("zkUrl", "localhost:2181").load()
df_ct_tmp = spark.read.format("org.apache.phoenix.spark").option("table","CITY_HB").option("zkUrl", "localhost:2181").load()
df_cn_tmp = spark.read.format("org.apache.phoenix.spark").option("table","COUNTRY_HB").option("zkUrl", "localhost:2181").load()
df_paid_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_PAID_HB").option("zkUrl", "localhost:2181").load()
df_txtype_tmp=spark.read.format("org.apache.phoenix.spark").option("table","TX_TYPE_HB").option("zkUrl", "localhost:2181").load()
df_debit_tmp=spark.read.format("org.apache.phoenix.spark").option("table","CC_DEBIT_HB").option("zkUrl", "localhost:2181").load()
#DSL
test=df_cd_tmp.join(df_ad_tmp,"add_id", how="left").join(df_ct_tmp,"ct_id", how="left")\
    .join(df_cn_tmp,"cn_id", how="left").drop("ADD_ID").drop("CT_ID").drop("CN_ID")

test1=df_paid_tmp.join(df_txtype_tmp,"tx_type_id", how="left").drop("TX_TYPE_ID")

#Give Alias

res=test.selectExpr("C_NUMBER as CardNumber","C_TYPE as CardType","FULL_NAME as full_name","MOB as contactnumber","EMAIL as emailId","STREET as address","CT_NAME as city","CN_NAME as country","ISSUE_DATE as issuedate","UPDATE_DATE as update_date","BILLING_DATE as BilligDate","C_LIMIT as CardLimit","ACT_FLAG as Active_flag")
res1=test1.selectExpr("C_number as CardNumber","tx_id as TransactionId","tx_date as transactionDate","amt_paid as amountpaid","c_status as cardstatus","tx_type_desc as transaction_type_desc")
#res2=df_debit_tmp.selectExpr("C_number as CardNumber","tx_id as TransactionId","tx_date as transactionDate","amt_paid as amountpaid","category as category","d_status as debitstatus")

#Write to hive

res.write.mode("overwrite").format("hive").saveAsTable("card_details_staging")
res1.write.mode("overwrite").format("hive").saveAsTable("credit_details_staging")
df_debit_tmp.write.mode("overwrite").format("hive").saveAsTable("debit_details_staging")
