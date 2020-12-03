from __future__ import print_function
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Borough Analysis_2").config("spark.some.config.option", "some-value").getOrCreate()

cvd_dhs = spark.read.format('csv').options(header='false', inferschema='false').load(sys.argv[1])

cvd_dhs = cvd_dhs.withColumnRenamed('_c0', 'date')
cvd_dhs = cvd_dhs.withColumnRenamed('_c1', 'case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c2', 'hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c3', 'death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c4', 'death_count_prob')
cvd_dhs = cvd_dhs.withColumnRenamed('_c5', 'bx_case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c6', 'bx_hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c7', 'bx_death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c8', 'bk_case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c9', 'bk_hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c10', 'bk_death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c11', 'mn_case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c12', 'mn_hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c13', 'mn_death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c14', 'qn_case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c15', 'qn_hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c16', 'qn_death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c17', 'si_case_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c18', 'si_hosp_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c19', 'si_death_count')
cvd_dhs = cvd_dhs.withColumnRenamed('_c20', 'bx_shelter_pop')
cvd_dhs = cvd_dhs.withColumnRenamed('_c21', 'bk_shelter_pop')
cvd_dhs = cvd_dhs.withColumnRenamed('_c22', 'mn_shelter_pop')
cvd_dhs = cvd_dhs.withColumnRenamed('_c23', 'qn_shelter_pop')
cvd_dhs = cvd_dhs.withColumnRenamed('_c24', 'si_shelter_pop')
cvd_dhs = cvd_dhs.withColumn(
    'date', functions.to_date(functions.unix_timestamp(cvd_dhs['date'], 'yyyy/MM/dd').cast(
        "timestamp"), 'yyyy/MM/dd'))

cvd_dhs = cvd_dhs.withColumn('bx_shelter_pop', cvd_dhs['bx_shelter_pop'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bk_shelter_pop', cvd_dhs['bk_shelter_pop'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('mn_shelter_pop', cvd_dhs['mn_shelter_pop'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('qn_shelter_pop', cvd_dhs['qn_shelter_pop'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('si_shelter_pop', cvd_dhs['si_shelter_pop'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('case_count', cvd_dhs['case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('hosp_count', cvd_dhs['hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('death_count', cvd_dhs['death_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('death_count_prob', cvd_dhs['death_count_prob'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bx_case_count', cvd_dhs['bx_case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bx_hosp_count', cvd_dhs['bx_hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bx_death_count', cvd_dhs['bx_death_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bk_case_count', cvd_dhs['bk_case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bk_hosp_count', cvd_dhs['bk_hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('bk_death_count', cvd_dhs['bk_death_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('mn_case_count', cvd_dhs['mn_case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('mn_hosp_count', cvd_dhs['mn_hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('mn_death_count', cvd_dhs['mn_death_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('qn_case_count', cvd_dhs['qn_case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('qn_hosp_count', cvd_dhs['qn_hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('qn_death_count', cvd_dhs['qn_death_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('si_case_count', cvd_dhs['si_case_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('si_hosp_count', cvd_dhs['si_hosp_count'].cast(types.IntegerType()))
cvd_dhs = cvd_dhs.withColumn('si_death_count', cvd_dhs['si_death_count'].cast(types.IntegerType()))

cvd_dhs = cvd_dhs.fillna(0, subset=['case_count', 'hosp_count', 'death_count', 'death_count_prob',
                                    'bx_case_count', 'bx_hosp_count', 'bx_death_count',
                                    'bk_case_count', 'bk_hosp_count', 'bk_death_count',
                                    'mn_case_count', 'mn_hosp_count', 'mn_death_count',
                                    'qn_case_count', 'qn_hosp_count', 'qn_death_count',
                                    'si_case_count', 'si_hosp_count', 'si_death_count'])

window = Window.partitionBy().orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
cvd_dhs = cvd_dhs.withColumn('cum_case_count', functions.sum(cvd_dhs['case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_hosp_count', functions.sum(cvd_dhs['hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_death_count', functions.sum(cvd_dhs['death_count']
                                                              + cvd_dhs['death_count_prob']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bx_case_count', functions.sum(cvd_dhs['bx_case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bx_hosp_count', functions.sum(cvd_dhs['bx_hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bx_death_count', functions.sum(cvd_dhs['bx_death_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bk_case_count', functions.sum(cvd_dhs['bk_case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bk_hosp_count', functions.sum(cvd_dhs['bk_hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_bk_death_count', functions.sum(cvd_dhs['bk_death_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_mn_case_count', functions.sum(cvd_dhs['mn_case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_mn_hosp_count', functions.sum(cvd_dhs['mn_hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_mn_death_count', functions.sum(cvd_dhs['mn_death_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_qn_case_count', functions.sum(cvd_dhs['qn_case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_qn_hosp_count', functions.sum(cvd_dhs['qn_hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_qn_death_count', functions.sum(cvd_dhs['qn_death_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_si_case_count', functions.sum(cvd_dhs['si_case_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_si_hosp_count', functions.sum(cvd_dhs['si_hosp_count']).over(window))
cvd_dhs = cvd_dhs.withColumn('cum_si_death_count', functions.sum(cvd_dhs['si_death_count']).over(window))

replace = functions.udf(lambda v: None if v == 'null' else v)
cvd_dhs = cvd_dhs.withColumn('bx_shelter_pop', replace(cvd_dhs['bx_shelter_pop']))
cvd_dhs = cvd_dhs.withColumn('bk_shelter_pop', replace(cvd_dhs['bk_shelter_pop']))
cvd_dhs = cvd_dhs.withColumn('mn_shelter_pop', replace(cvd_dhs['mn_shelter_pop']))
cvd_dhs = cvd_dhs.withColumn('qn_shelter_pop', replace(cvd_dhs['qn_shelter_pop']))
cvd_dhs = cvd_dhs.withColumn('si_shelter_pop', replace(cvd_dhs['si_shelter_pop']))

window2 = Window.partitionBy().orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
cvd_dhs = cvd_dhs.withColumn('bx_pop_rolling_avg',
                             functions.mean(cvd_dhs['bx_shelter_pop']).over(window2))
cvd_dhs = cvd_dhs.withColumn('bk_pop_rolling_avg',
                             functions.mean(cvd_dhs['bk_shelter_pop']).over(window2))
cvd_dhs = cvd_dhs.withColumn('mn_pop_rolling_avg',
                             functions.mean(cvd_dhs['mn_shelter_pop']).over(window2))
cvd_dhs = cvd_dhs.withColumn('qn_pop_rolling_avg',
                             functions.mean(cvd_dhs['qn_shelter_pop']).over(window2))
cvd_dhs = cvd_dhs.withColumn('si_pop_rolling_avg',
                             functions.mean(cvd_dhs['si_shelter_pop']).over(window2))

cvd_dhs.createOrReplaceTempView('cvd_dhs')

result = spark.sql("SELECT CD.date, CD.case_count, CD.cum_case_count, "
                   "CD.hosp_count, "
                   "CD.cum_hosp_count, CD.death_count, CD.cum_death_count, "
                   "CD.bx_case_count, CD.cum_bx_case_count, CD.bx_hosp_count, "
                   "CD.cum_bx_hosp_count, CD.bx_death_count, CD.cum_bx_death_count, "
                   "CD.bk_case_count, CD.cum_bk_case_count, CD.bk_hosp_count, "
                   "CD.cum_bk_hosp_count, CD.bk_death_count, CD.cum_bk_death_count, "
                   "CD.mn_case_count, CD.cum_mn_case_count, CD.mn_hosp_count, "
                   "CD.cum_mn_hosp_count, CD.mn_death_count, CD.cum_mn_death_count, "
                   "CD.qn_case_count, CD.cum_qn_case_count, CD.qn_hosp_count, "
                   "CD.cum_qn_hosp_count, CD.qn_death_count, CD.cum_qn_death_count, "
                   "CD.si_case_count, CD.cum_si_case_count, CD.si_hosp_count, "
                   "CD.cum_si_hosp_count, CD.si_death_count, CD.cum_si_death_count, "
                   "AVG(CD3.bx_shelter_pop) as bx_avg, AVG(CD3.bk_shelter_pop) as bk_avg, "
                   "AVG(CD3.mn_shelter_pop) as mn_avg, AVG(CD3.qn_shelter_pop) as qn_avg, "
                   "AVG(CD3.si_shelter_pop) as si_avg, COALESCE(CD.bx_shelter_pop, bx_pop_rolling_avg) as bx_cvd, "
                   "COALESCE(CD.bk_shelter_pop, bk_pop_rolling_avg) as bk_cvd, "
                   "COALESCE(CD.mn_shelter_pop, mn_pop_rolling_avg) as mn_cvd, "
                   "COALESCE(CD.qn_shelter_pop, qn_pop_rolling_avg) as qn_cvd, "
                   "COALESCE(CD.si_shelter_pop, si_pop_rolling_avg) as si_cvd, "
                   "((COALESCE(CD.bx_shelter_pop, bx_pop_rolling_avg) - AVG(CD3.bx_shelter_pop)) / "
                   "AVG(CD3.bx_shelter_pop)) as bx_pscore, "
                   "((COALESCE(CD.bk_shelter_pop, bk_pop_rolling_avg) - AVG(CD3.bk_shelter_pop)) / "
                   "AVG(CD3.bk_shelter_pop)) as bk_pscore, "
                   "((COALESCE(CD.mn_shelter_pop, mn_pop_rolling_avg) - AVG(CD3.mn_shelter_pop)) / "
                   "AVG(CD3.mn_shelter_pop)) as mn_pscore, "
                   "((COALESCE(CD.qn_shelter_pop, qn_pop_rolling_avg) - AVG(CD3.qn_shelter_pop)) / "
                   "AVG(CD3.qn_shelter_pop)) as qn_pscore, "
                   "((COALESCE(CD.si_shelter_pop, si_pop_rolling_avg) - AVG(CD3.si_shelter_pop)) / "
                   "AVG(CD3.si_shelter_pop)) as si_pscore "
                   "FROM cvd_dhs CD LEFT OUTER JOIN (SELECT CD2.date, CD2.bx_shelter_pop, CD2.bk_shelter_pop, "
                   "CD2.mn_shelter_pop, CD2.qn_shelter_pop, CD2.si_shelter_pop "
                   "FROM cvd_dhs CD2 WHERE YEAR(CD2.date) >= 2018 AND YEAR(CD2.date) < 2020) CD3 "
                   "ON (MONTH(CD.date) = MONTH(CD3.date) AND DAY(CD.date) = DAY(CD3.date)) "
                   "WHERE YEAR(CD.date) >= 2020 AND MONTH(CD.date) >= 2 "
                   "GROUP BY CD.date, CD.case_count, CD.cum_case_count, CD.hosp_count, "
                   "CD.cum_hosp_count, CD.death_count, CD.cum_death_count, "
                   "CD.bx_case_count, CD.cum_bx_case_count, CD.bx_hosp_count, "
                   "CD.cum_bx_hosp_count, CD.bx_death_count, CD.cum_bx_death_count, "
                   "CD.bk_case_count, CD.cum_bk_case_count, CD.bk_hosp_count, "
                   "CD.cum_bk_hosp_count, CD.bk_death_count, CD.cum_bk_death_count, "
                   "CD.mn_case_count, CD.cum_mn_case_count, CD.mn_hosp_count, "
                   "CD.cum_mn_hosp_count, CD.mn_death_count, CD.cum_mn_death_count, "
                   "CD.qn_case_count, CD.cum_qn_case_count, CD.qn_hosp_count, "
                   "CD.cum_qn_hosp_count, CD.qn_death_count, CD.cum_qn_death_count, "
                   "CD.si_case_count, CD.cum_si_case_count, CD.si_hosp_count, "
                   "CD.cum_si_hosp_count, CD.si_death_count, CD.cum_si_death_count, bx_cvd, "
                   "bk_cvd, mn_cvd, qn_cvd, si_cvd "
                   "ORDER BY CD.date")

result = result.withColumn('date', functions.date_format(result['date'], 'MM/dd/yyyy'))

result.select(functions.format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                                      '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'
                                      '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',
                                      result.date, result.case_count,
                                      result.cum_case_count, result.hosp_count, result.cum_hosp_count,
                                      result.death_count, result.cum_death_count, result.bx_case_count,
                                      result.cum_bx_case_count, result.bx_hosp_count, result.cum_bx_hosp_count,
                                      result.bx_death_count, result.cum_bx_death_count, result.bk_case_count,
                                      result.cum_bk_case_count, result.bk_hosp_count, result.cum_bk_hosp_count,
                                      result.bk_death_count, result.cum_bk_death_count, result.mn_case_count,
                                      result.cum_mn_case_count, result.mn_hosp_count, result.cum_mn_hosp_count,
                                      result.mn_death_count, result.cum_mn_death_count, result.qn_case_count,
                                      result.cum_qn_case_count, result.qn_hosp_count, result.cum_qn_hosp_count,
                                      result.qn_death_count, result.cum_qn_death_count, result.si_case_count,
                                      result.cum_si_case_count, result.si_hosp_count, result.cum_si_hosp_count,
                                      result.si_death_count, result.cum_si_death_count, result.bx_avg,
                                      result.bk_avg, result.mn_avg, result.qn_avg, result.si_avg, result.bx_cvd,
                                      result.bk_cvd, result.mn_cvd, result.qn_cvd, result.si_cvd, result.bx_pscore,
                                      result.bk_pscore, result.mn_pscore, result.qn_pscore, result.si_pscore
                                      )).\
    write.save('excess_homelessness_borough', format="text")

spark.stop()

"""
result = spark.sql("SELECT CD.date, CD.borough, FIRST(CD.case_count), FIRST(CD.cum_case_count), FIRST(CD.hosp_count), "
                   "FIRST(CD.cum_hosp_count), FIRST(CD.death_count), FIRST(CD.cum_death_count), "
                   "FIRST(CD.bx_case_count), FIRST(CD.cum_bx_case_count), FIRST(CD.bx_hosp_count), "
                   "FIRST(CD.cum_bx_hosp_count), FIRST(CD.bx_death_count), FIRST(CD.cum_bx_death_count), "
                   "FIRST(CD.bk_case_count), FIRST(CD.cum_bk_case_count), FIRST(CD.bk_hosp_count), "
                   "FIRST(CD.cum_bk_hosp_count), FIRST(CD.bk_death_count), FIRST(CD.cum_bk_death_count), "
                   "FIRST(CD.mn_case_count), FIRST(CD.cum_mn_case_count), FIRST(CD.mn_hosp_count), "
                   "FIRST(CD.cum_mn_hosp_count), FIRST(CD.mn_death_count), FIRST(CD.cum_mn_death_count), "
                   "FIRST(CD.qn_case_count), FIRST(CD.cum_qn_case_count), FIRST(CD.qn_hosp_count), "
                   "FIRST(CD.cum_qn_hosp_count), FIRST(CD.qn_death_count), FIRST(CD.cum_qn_death_count), "
                   "FIRST(CD.si_case_count), FIRST(CD.cum_si_case_count), FIRST(CD.si_hosp_count), "
                   "FIRST(CD.cum_si_hosp_count), FIRST(CD.si_death_count), FIRST(CD.cum_si_death_count), "
                   "AVG(CD3.total_shelter_pop) as pop_avg, FIRST(CD.total_shelter_pop) as pop_cvd, "
                   "((FIRST(CD.total_shelter_pop) - AVG(CD3.total_shelter_pop)) / AVG(CD3.total_shelter_pop)) "
                   "as pop_pscore "
                   "FROM (SELECT CD2.date, CD2.borough, CD2.total_shelter_pop FROM cvd_dhs CD2 "
                   "WHERE YEAR(CD2.date) >= 2018 AND YEAR(CD2.date) < 2020) CD3 "
                   "INNER JOIN cvd_dhs CD ON (MONTH(CD.date) = MONTH(CD3.date) AND DAY(CD.date) = DAY(CD3.date) "
                   "AND CD3.borough = CD.borough) "
                   "WHERE YEAR(CD.date) >= 2020 AND MONTH(CD.date) >= 2 "
                   "GROUP BY CD.date, CD.borough "
                   "ORDER BY CD.date, CD.borough")
"""
