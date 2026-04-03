#from pyspark.sql import SparkSession
from pyspark.sql.functions import sum
spark = SparkSession.builder.appName("Phase2").getOrCreate()
------------------------------------------------------------
First Pipeline : Read sales data -> clean nulls -> calculate daily sales
#Read sales data
orders = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')
#clean nulls
orders = orders.dropna(subset=["customer_id", "total_amount", "sale_date"])
#calculate daily sales
