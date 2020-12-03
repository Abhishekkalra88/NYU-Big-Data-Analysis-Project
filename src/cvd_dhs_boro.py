from __future__ import print_function
import sys
from pyspark.sql import SparkSession, functions

spark = SparkSession.builder.appName("borough analysis").config("spark.some.config.option", "some-value").getOrCreate()

covid = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
dhs = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
dates = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[3])

covid = covid.withColumnRenamed('DEATH_COUNT_PROBABLE	', 'DEATH_COUNT_PROBABLE')

covid = covid.withColumn(
    'DATE_OF_INTEREST', functions.date_format(functions.unix_timestamp(covid['DATE_OF_INTEREST'], 'MM/dd/yy').cast(
        "timestamp"), 'yyyy/MM/dd'))

dhs = dhs.withColumn(
    'Report_Date', functions.date_format(functions.unix_timestamp(dhs['Report_Date'], 'MM/dd/yy').cast(
        "timestamp"), 'yyyy/MM/dd'))
dhs = dhs.withColumn('Borough', functions.regexp_replace('Borough', 'Staten Island', 'Staten_Island'))

dhs = dhs.fillna(0)
pivot_dhs = dhs.groupby('Report_date').pivot('Borough').agg(
    functions.sum('Adult_Family_Commercial_Hotel').alias('Adult_Family_Commercial_Hotel'),
    functions.sum('Adult_Family_Shelter').alias('Adult_Family_Shelter'),
    functions.sum('Adult_Shelter').alias('Adult_Shelter'),
    functions.sum('Adult_Shelter_Commercial_Hotel').alias('Adult_Shelter_Commercial_Hotel'),
    functions.sum('Family_Cluster').alias('Family_Cluster'),
    functions.sum('Family_with_Children_Commercial_Hotel').alias('Family_with_Children_Commercial_Hotel'),
    functions.sum('Family_with_Children_Shelter').alias('Family_with_Children_Shelter'))

dates = dates.withColumn(
    'date', functions.date_format(functions.unix_timestamp(dates['date'], 'yyyy/MM/dd').cast(
        "timestamp"), 'yyyy/MM/dd'))

covid.createOrReplaceTempView('covid')
pivot_dhs.createOrReplaceTempView('dhs')
dates.createOrReplaceTempView('dates')

result = spark.sql("SELECT DISTINCT DD.date, C.*, bx_shelter_pop, bk_shelter_pop, mn_shelter_pop, "
                   "qn_shelter_pop, si_shelter_pop "
                   "FROM (SELECT D.date, "
                   "(Bronx_Adult_Family_Commercial_Hotel + Bronx_Adult_Family_Shelter + Bronx_Adult_Shelter "
                   "+ Bronx_Adult_Shelter_Commercial_Hotel + Bronx_Family_Cluster "
                   "+ Bronx_Family_with_Children_Commercial_Hotel "
                   "+ Bronx_Family_with_Children_Shelter) as bx_shelter_pop,  "
                   "(Brooklyn_Adult_Family_Commercial_Hotel + Brooklyn_Adult_Family_Shelter "
                   "+ Brooklyn_Adult_Shelter + Brooklyn_Adult_Shelter_Commercial_Hotel + Brooklyn_Family_Cluster "
                   "+ Brooklyn_Family_with_Children_Commercial_Hotel "
                   "+ Brooklyn_Family_with_Children_Shelter) as bk_shelter_pop, "
                   "(Manhattan_Adult_Family_Commercial_Hotel + Manhattan_Adult_Family_Shelter "
                   "+ Manhattan_Adult_Shelter + Manhattan_Adult_Shelter_Commercial_Hotel + Manhattan_Family_Cluster "
                   "+ Manhattan_Family_with_Children_Commercial_Hotel "
                   "+ Manhattan_Family_with_Children_Shelter) as mn_shelter_pop, "
                   "(Queens_Adult_Family_Commercial_Hotel + Queens_Adult_Family_Shelter "
                   "+ Queens_Adult_Shelter + Queens_Adult_Shelter_Commercial_Hotel + Queens_Family_Cluster "
                   "+ Queens_Family_with_Children_Commercial_Hotel "
                   "+ Queens_Family_with_Children_Shelter) as qn_shelter_pop, "
                   "(Staten_Island_Adult_Family_Commercial_Hotel + Staten_Island_Adult_Family_Shelter "
                   "+ Staten_Island_Adult_Shelter + Staten_Island_Adult_Shelter_Commercial_Hotel "
                   "+ Staten_Island_Family_Cluster + Staten_Island_Family_with_Children_Commercial_Hotel "
                   "+ Staten_Island_Family_with_Children_Shelter) as si_shelter_pop "
                   "FROM dates D LEFT OUTER JOIN dhs DH on D.date = DH.Report_Date) DD "
                   "LEFT OUTER JOIN COVID C "
                   "ON C.DATE_OF_INTEREST = DD.date "
                   "ORDER BY DD.date")

result.select(functions.format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,',
              result.date, result.CASE_COUNT, result.HOSPITALIZED_COUNT, result.DEATH_COUNT,
                                      result.DEATH_COUNT_PROBABLE, result.BX_CASE_COUNT,
                                      result.BX_HOSPITALIZED_COUNT, result.BX_DEATH_COUNT, result.BK_CASE_COUNT,
                                      result.BK_HOSPITALIZED_COUNT, result.BK_DEATH_COUNT, result.MN_CASE_COUNT,
                                      result.MN_HOSPITALIZED_COUNT, result.MN_DEATH_COUNT, result.QN_CASE_COUNT,
                                      result.QN_HOSPITALIZED_COUNT, result.QN_DEATH_COUNT, result.SI_CASE_COUNT,
                                      result.SI_HOSPITALIZED_COUNT, result.SI_DEATH_COUNT, result.bx_shelter_pop,
                                      result.bk_shelter_pop, result.mn_shelter_pop, result.qn_shelter_pop,
                                      result.si_shelter_pop
                                      )).write.save('covid_dhs_borough', format='text')

spark.stop()
