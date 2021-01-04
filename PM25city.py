# -*- coding: UTF-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions
import math

spark = SparkSession\
    .builder\
    .appName('PM25city')\
    .getOrCreate()

file_name = 'hdfs://localhost:9000/usr/hadoop/PM25city'

schema =  StructType([
        StructField("station_id",IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
	StructField("PM25", IntegerType(), True),
	StructField("PM10", IntegerType(), True),
	StructField("NO2", IntegerType(), True),
	StructField("SO2", IntegerType(), True),
	StructField("O3_1", IntegerType(), True),
	StructField("O3_8h", IntegerType(), True),
	StructField("CO", DoubleType(), True),
	StructField("AQI", IntegerType(), True),
	StructField("level", IntegerType(), True),
	StructField("year", IntegerType(), True),
	StructField("month", IntegerType(), True),
	StructField("date", IntegerType(), True),
	StructField("hour", IntegerType(), True),
	StructField("city", StringType(), True),
        ])

init_df = spark.read.csv(file_name, 
                        encoding='utf-8', 
                        header=True, 
                        schema=schema,
                        sep=',')

# 利用DataFrame为init_df创建临时视图
init_df.createOrReplaceTempView("init_df")

#(1)
# 计算每个城市所有时刻的平均PM25来衡量PM25的高低程度
spark.sql("select avg(PM25) as avg_PM25, city from init_df group by city order by avg_PM25 desc").show()

#(2)
# 统计2019年2月需要计算的指标数量
day_count = spark.sql('select avg(SO2) as SO2, avg(NO2) as NO2, avg(PM10) as PM10, avg(PM25) as PM25, avg(CO) as CO, max(O3_1) as O3_1, max(O3_8h) as O3_8h, avg(year) as year, avg(month) as month, date, city from init_df where year=2019 and month=2 and city in ("北京", "上海", "成都") group by city, date')


# 计算污染物X的IAQI
def IAQI_X(IAQI_H, IAQI_L, BP_H, BP_L, C):
        return (IAQI_H-IAQI_L)/(BP_H-BP_L)*(C-BP_L)+IAQI_L

# 计算某种污染物的日IAQI指数
def day_IAQI(val, name):
        IAQI = [0, 50, 100, 150, 200, 300, 400, 500]
        SO2 = [0,50,150,475,800,1600,2100,2620]
        NO2 = [0,40,80,180,280,565,750,940]
        PM10 = [0,50,150,250,350,420,500,600]
        CO = [0,2,4,14,24,36,48,60]
        O3_1 = [0,160,200,300,400,800,1000,1200]
        # 超过800按照O3_h计算
        O3_8h = [0,100,160,215,265,800,800]
        PM25 = [0,35,75,115,150,250,350,500]

        # 依据名字选择列表
        item = locals()[name]
        for i in range(len(item)-1):
                j = i+1
		# 此时为O3_8h超过800
                if (item[i]==item[j]):
                        return -1;
                if(item[i]<= val <item[j]):
                        temp = IAQI_X(IAQI[i], IAQI[j], item[i], item[j], val)
                        return math.ceil(temp)
        # 单项超过上限值肯定为重度污染，设置IAQI为666(此数据中日平均没有)
        print(val, name) 
        return 666

# 计算每日的AQI
def day_AQI(SO2, NO2, PM10, CO, O3_1, O3_8h, PM25):
        ls = [SO2, NO2, PM10, CO, O3_1, O3_8h, PM25]
        names = ["SO2", "NO2", "PM10", "CO", "O3_1", "O3_8h", "PM25"]
        return  max([day_IAQI(x, name) for name, x in zip(names,ls)])

my_udf_AQI = functions.udf(day_AQI, IntegerType())
day_count_AQI = day_count.withColumn("AQI", my_udf_AQI(day_count.SO2, day_count.NO2, day_count.PM10, day_count.CO, day_count.O3_1, day_count.O3_8h, day_count.PM25))

#day_count_AQI.show()

# 某天空气质量等级
def day_level(AQI):
        level = [0, 50, 100, 200, 300, 300]
        for i in range(len(level)-1):
                j = i+1
                if (level[i]==level[j]):
                        return 5;
                if(level[i]< AQI <=level[j]):
                        return j;

my_udf_level = functions.udf(day_level, IntegerType())
day_count_level = day_count_AQI["city", "date", "AQI"].withColumn("level", my_udf_level(day_count_AQI.AQI))
#day_count_level.show()
day_count_level.createOrReplaceTempView("day_count_level")

spark.sql("select city, level, count(*) as amount from day_count_level group by city,level order by city,level").show()

#(3)

