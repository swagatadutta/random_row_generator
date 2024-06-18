from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql import Row

import random
from itertools import repeat

WRITE_PATH = 'dpms_orc/'

spark = SparkSession.builder \
    .appName("DPMS Random data") \
    .master("local[*]") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "512m") \
    .config("spark.driver.cores", "1") \
    .config("spark.executor.instances", "1") \
    .config("spark.executor.memory", "10g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()


def dummy_generator(l):
    v1 = random.randint(10000000,20000000)
    v2 = random.randint(1,172)
    v3 = random.randint(1,969)
    v4 = random.randint(1,157)
    v5 = random.randint(1,2)
    v6 = random.randint(0,11)

    return Row(col1=str(imei), 
               col2=str(verblobid), 
               col3=str(apn), 
               col4=str(an), 
               col5=str(region), 
               col6=str(ut))

total_rows = 10000000000
chunksize = 100000000
num_partitions = int(total_rows/chunksize)

rdd = spark.sparkContext.parallelize(range(total_rows), num_partitions).map(dummy_generator)

schema_data = {"fields":[{"metadata":{},"name":"v1","nullable":True,"type":"string"},
                    {"metadata":{},"name":"v2","nullable":True,"type":"string"},
                    {"metadata":{},"name":"v3","nullable":True,"type":"string"},
                    {"metadata":{},"name":"v4","nullable":True,"type":"string"},
                    {"metadata":{},"name":"v5","nullable":True,"type":"string"},
                    {"metadata":{},"name":"v6","nullable":True,"type":"string"}],
             "type":"struct"}

schema = StructType.fromJson(schema_data)

df=spark.createDataFrame(rdd, schema)

df.write.mode('overwrite').orc(WRITE_PATH)


