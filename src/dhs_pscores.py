from __future__ import print_function
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("task2").config("spark.some.config.option", "some-value").getOrCreate()

cvd_dhs = spark.read.format('csv').options(header='false', inferschema='false').load(sys.argv[1])

cvd_dhs = cvd_dhs.withColumnRenamed('_c0', 'date')
cvd_dhs = cvd_dhs.withColumnRenamed('_c1', 'Total_Adults_in_Shelter')
cvd_dhs = cvd_dhs.withColumnRenamed('_c2', 'Total_Children_in_Shelter')
cvd_dhs = cvd_dhs.withColumnRenamed('_c3', 'Total_Individuals_in_Shelter')
cvd_dhs = cvd_dhs.withColumnRenamed('_c13', 'CASE_COUNT')
cvd_dhs = cvd_dhs.withColumnRenamed('_c14', 'HOSPITALIZED_COUNT')
cvd_dhs = cvd_dhs.withColumnRenamed('_c15', 'DEATH_COUNT')
cvd_dhs = cvd_dhs.withColumnRenamed('_c16', 'DEATH_COUNT_PROBABLE')
cvd_dhs = cvd_dhs.withColumn(
    'date', functions.to_date(functions.unix_timestamp(cvd_dhs['date'], 'yyyy/MM/dd').cast(
        "timestamp"), 'yyyy/MM/dd'))

cvd_dhs = cvd_dhs.withColumn('CASE_COUNT', cvd_dhs['CASE_COUNT'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('HOSPITALIZED_COUNT', cvd_dhs['HOSPITALIZED_COUNT'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('DEATH_COUNT', cvd_dhs['DEATH_COUNT'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('DEATH_COUNT_PROBABLE', cvd_dhs['DEATH_COUNT_PROBABLE'].cast(types.IntegerType()))

cvd_dhs = cvd_dhs.fillna(0, subset=['CASE_COUNT', 'HOSPITALIZED_COUNT', 'DEATH_COUNT', 'DEATH_COUNT_PROBABLE'])
window = Window.partitionBy().orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
cvd_dhs = cvd_dhs.withColumn('cum_case_count', functions.sum(cvd_dhs['CASE_COUNT']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_hosp_count', functions.sum(cvd_dhs['HOSPITALIZED_COUNT']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_death_count', functions.sum(cvd_dhs['DEATH_COUNT']
                                                              + cvd_dhs['DEATH_COUNT_PROBABLE']).over(window))

replace = functions.udf(lambda v: None if v == 'null' else v)
cvd_dhs = cvd_dhs.withColumn('Total_Adults_in_Shelter', replace(cvd_dhs['Total_Adults_in_Shelter']))
cvd_dhs = cvd_dhs.withColumn('Total_Children_in_Shelter', replace(cvd_dhs['Total_Children_in_Shelter']))
cvd_dhs = cvd_dhs.withColumn('Total_Individuals_in_Shelter', replace(cvd_dhs['Total_Individuals_in_Shelter']))

window2 = Window.partitionBy().orderBy('date').rowsBetween(Window.currentRow - 8, Window.currentRow - 1)
cvd_dhs = cvd_dhs.withColumn('total_adults_rolling_avg',
                             functions.mean(cvd_dhs['Total_Adults_in_Shelter']).over(window2))
cvd_dhs = cvd_dhs.withColumn('total_children_rolling_avg',
                             functions.mean(cvd_dhs['Total_Children_in_Shelter']).over(window2))
cvd_dhs = cvd_dhs.withColumn('total_individuals_rolling_avg',
                             functions.mean(cvd_dhs['Total_Individuals_in_Shelter']).over(window2))

cvd_dhs.createOrReplaceTempView('cvd_dhs')
result = spark.sql("SELECT CD.date, CD.CASE_COUNT, CD.cum_case_count, CD.HOSPITALIZED_COUNT, "
                   "CD.cum_hosp_count, CD.DEATH_COUNT, CD.DEATH_COUNT_PROBABLE, CD.cum_death_count, "
                   "AVG(CD3.adults) as adults_avg, AVG(CD3.children) as children_avg, AVG(CD3.total) as total_avg, "
                   "COALESCE(CD.Total_Adults_in_Shelter, CD.total_adults_rolling_avg) as adults_cvd, "
                   "COALESCE(CD.Total_Children_in_Shelter, CD.total_children_rolling_avg) as children_cvd, "
                   "COALESCE(CD.Total_Individuals_in_Shelter, CD.total_individuals_rolling_avg) as total_cvd, "
                   "((COALESCE(CD.Total_Adults_in_Shelter, CD.total_adults_rolling_avg) - "
                   "AVG(CD3.adults)) / AVG(CD3.adults)) as adults_pscore, "
                   "((COALESCE(CD.Total_Children_in_Shelter, CD.total_children_rolling_avg) - "
                   "AVG(CD3.children)) / AVG(CD3.children)) as children_pscore, "
                   "((COALESCE(CD.Total_Individuals_in_Shelter, CD.total_individuals_rolling_avg) "
                   "- AVG(CD3.total)) / AVG(CD3.total)) as total_pscore "
                   "FROM (SELECT CD2.date, CD2.Total_Adults_in_Shelter as adults, "
                   "CD2.Total_Children_in_Shelter as children, CD2.Total_Individuals_in_Shelter as total "
                   "FROM cvd_dhs CD2 WHERE YEAR(CD2.date) >= 2014 AND YEAR(CD2.date) < 2020) CD3 "
                   "INNER JOIN cvd_dhs CD ON (MONTH(CD.date) = MONTH(CD3.date) AND DAY(CD.date) = DAY(CD3.date)) "
                   "WHERE YEAR(CD.date) >= 2020 AND MONTH(CD.date) >= 2 "
                   "GROUP BY month_name, month_day, CD.date, CD.CASE_COUNT, CD.cum_case_count, CD.HOSPITALIZED_COUNT, "
                   "CD.cum_hosp_count, CD.DEATH_COUNT, CD.DEATH_COUNT_PROBABLE, CD.cum_death_count, "
                   "adults_cvd, children_cvd, total_cvd "
                   "ORDER BY CD.date")

result = result.withColumn('date', functions.date_format(result['date'], 'MM/dd/yyyy'))

result.select(functions.format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
                                      result.date, result.month_day, result.CASE_COUNT,
                                      result.cum_case_count, result.HOSPITALIZED_COUNT, result.cum_hosp_count,
                                      result.DEATH_COUNT, result.DEATH_COUNT_PROBABLE, result.cum_death_count,
                                      result.adults_avg, result.children_avg, result.total_avg,
                                      result.adults_cvd, result.children_cvd, result.total_cvd, result.adults_pscore,
                                      result.children_pscore, result.total_pscore)).\
    write.save('excess_homelessness', format="text")

spark.stop()
