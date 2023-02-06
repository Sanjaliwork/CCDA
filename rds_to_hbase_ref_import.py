import sys
import os
import findspark
os.environ["JAVA_HOME"]="/usr/lib/jvm/java"
os.environ["SPARK_HOME"]="/usr/lib/spark"
from pyspark.sql import *
import re
from pyspark.sql.functions import *
spark =SparkSession.builder.master("local[*]").appName("test").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("Importing data from Postgrage Database Start")
p="myppassword"
user="mypuser"
pp="jdbc:postgresql://mypdbinstance.cf0mht5l44yb.ap-south-1.rds.amazonaws.com:5432/PROD"
driver="org.postgresql.Driver"

print("Start Process..........COUNTRY.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","country").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","COUNTRY_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")

print("Start Process.........CARD.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","card").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","CARD_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")

print("Start Process..........CITY.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","city").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","CITY_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")


print("Start Process..........ADDRESS.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","address").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","ADDRESS_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")


print("Start Process..........CARD_TYPE............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","card_type").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","CARD_TYPE_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")


print("Start Process.........TX_TYPE.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","tx_type").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","TX_TYPE_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")


print("Start Process..........CC_PAID.............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","cc_paid").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","CC_PAID_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")


print("Start Process..........CC_DEBIT............")
df_country=spark.read.format("jdbc").option("url",pp).option("user", user).option("password",p).option("driver",driver).option("dbtable","cc_debit").load()
print("country Table Import Done........................")
print(" Start country Table Exporting To Hbase")
df_country.write.format("org.apache.phoenix.spark").option("zkUrl","localhost:2181").option("table","CC_DEBIT_HB").mode("overwrite").save()
print("Done country Table Exporting.........................")
