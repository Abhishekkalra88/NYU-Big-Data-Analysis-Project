from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions

spark = SparkSession.builder.appName("task1").config("spark.some.config.option", "some-value").getOrCreate()

covid = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
dhs = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
dates = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[3])

covid = covid.withColumnRenamed('DEATH_COUNT_PROBABLE	', 'DEATH_COUNT_PROBABLE')

covid = covid.withColumn(
    'DATE_OF_INTEREST', functions.date_format(functions.unix_timestamp(covid['DATE_OF_INTEREST'], 'MM/dd/yy').cast(
        "timestamp"), 'yyyy/MM/dd'))

dhs = dhs.withColumn(
    'Date_of_Census', functions.date_format(functions.unix_timestamp(dhs['Date_of_Census'], 'MM/dd/yy').cast(
        "timestamp"), 'yyyy/MM/dd'))

dates = dates.withColumn(
    'date', functions.date_format(functions.unix_timestamp(dates['date'], 'yyyy/MM/dd').cast(
        "timestamp"), 'yyyy/MM/dd'))

covid.createOrReplaceTempView('covid')
dhs.createOrReplaceTempView('dhs')
dates.createOrReplaceTempView('dates')

result = spark.sql("SELECT DISTINCT * "
                   "FROM (SELECT * FROM dates D LEFT OUTER JOIN dhs DH on D.date = DH.Date_of_Census) DD "
                   "LEFT OUTER JOIN COVID C "
                   "ON C.DATE_OF_INTEREST = DD.date "
                   "ORDER BY DD.date")

result.select(functions.format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
              result.date, result.Total_Adults_in_Shelter, result.Total_Children_in_Shelter,
              result.Total_Individuals_in_Shelter, result.Single_Adult_Men_in_Shelter,
              result.Single_Adult_Women_in_Shelter, result.Total_Single_Adults_in_Shelter,
              result.Families_with_Children_in_Shelter, result.Adults_in_Families_with_Children_in_Shelter,
              result.Children_in_Families_with_Children_in_Shelter,
              result.Total_Individuals_in_Families_with_Children_in_Shelter_, result.Adult_Families_in_Shelter,
              result.Individuals_in_Adult_Families_in_Shelter, result.CASE_COUNT, result.HOSPITALIZED_COUNT,
              result.DEATH_COUNT, result.DEATH_COUNT_PROBABLE)).write.save('covid_dhs_out', format='text')

spark.stop()
