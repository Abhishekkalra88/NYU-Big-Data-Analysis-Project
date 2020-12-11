# importing the modules
from __future__ import print_function
import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.sql.window import Window

# importing the datasets as pyspark dataframes
spark = SparkSession.builder.appName("covid_nyc_analysis").config("spark.some.config.option",
                                                                  "some-value").getOrCreate()


# helper functions
def time_format(df, x, f):
    return df.withColumn(x, functions.date_format(functions.unix_timestamp(df[x], f).cast("timestamp"), 'yyyy-MM-dd'))


def clean_na(df, X):
    replace = functions.udf(lambda v: None if v == 'null' else v)
    for x in X:
        df = df.withColumn(x, replace(df[x]))
    return df


def cum_count(df, X, w):
    pre = 'cum_'
    for x in X:
        if x == 'death_count':
            post = '_prob'
            df = df.withColumn(pre + x, functions.sum(df[x] + df[x + post]).over(w))
        else:
            df = df.withColumn(pre + x, functions.sum(df[x]).over(w))
    return df


def col_cast(df, X, t):
    for x in X:
        df = df.withColumn(x, df[x].cast(t))
    return df


# Read in and clean COVID-19 Cases, Hospitalization, and deaths data
covid = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
covid = time_format(covid, 'DATE_OF_INTEREST', 'MM/dd/yy')

rename = [('CASE_COUNT', 'case_count'), ('HOSPITALIZED_COUNT', 'hosp_count'), ('DEATH_COUNT', 'death_count'),
          ('DEATH_COUNT_PROBABLE	', 'death_count_prob'), ('BX_CASE_COUNT', 'bx_case_count'),
          ('BX_HOSPITALIZED_COUNT', 'bx_hosp_count'), ('BX_DEATH_COUNT', 'bx_death_count'),
          ('BK_CASE_COUNT', 'bk_case_count'), ('BK_HOSPITALIZED_COUNT', 'bk_hosp_count'),
          ('BK_DEATH_COUNT', 'bk_death_count'), ('MN_CASE_COUNT', 'mn_case_count'),
          ('MN_HOSPITALIZED_COUNT', 'mn_hosp_count'), ('MN_DEATH_COUNT', 'mn_death_count'),
          ('QN_CASE_COUNT', 'qn_case_count'), ('QN_HOSPITALIZED_COUNT', 'qn_hosp_count'),
          ('QN_DEATH_COUNT', 'qn_death_count'), ('SI_CASE_COUNT', 'si_case_count'),
          ('SI_HOSPITALIZED_COUNT', 'si_hosp_count'), ('SI_DEATH_COUNT', 'si_death_count')]
for name in rename:
    covid = covid.withColumnRenamed(name[0], name[1])

covid = covid.fillna(0, subset=['case_count', 'hosp_count', 'death_count', 'death_count_prob'])
cols = ['case_count', 'hosp_count', 'death_count', 'death_count_prob']
covid = col_cast(covid, cols, types.IntegerType())
window = Window.partitionBy().orderBy('DATE_OF_INTEREST').rowsBetween(Window.unboundedPreceding, Window.currentRow)
covid = cum_count(covid, cols, window)

# Read in and clean DHS Daily Report
dhs = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
dhs = time_format(dhs, 'Date_of_Census', 'MM/dd/yy')

# Read in and clean DHS Census by Borough data
dhs_borough = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[3])
dhs_borough = time_format(dhs_borough, 'Report_Date', 'MM/dd/yy')
dhs_borough = dhs_borough.withColumn('Borough', functions.regexp_replace('Borough', 'Staten Island', 'Staten_Island'))

dhs_borough = dhs_borough.fillna(0)
pivot_dhs_borough = dhs_borough.groupby('Report_date').pivot('Borough').agg(
    functions.sum('Adult_Family_Commercial_Hotel').alias('Adult_Family_Commercial_Hotel'),
    functions.sum('Adult_Family_Shelter').alias('Adult_Family_Shelter'),
    functions.sum('Adult_Shelter').alias('Adult_Shelter'),
    functions.sum('Adult_Shelter_Commercial_Hotel').alias('Adult_Shelter_Commercial_Hotel'),
    functions.sum('Family_Cluster').alias('Family_Cluster'),
    functions.sum('Family_with_Children_Commercial_Hotel').alias('Family_with_Children_Commercial_Hotel'),
    functions.sum('Family_with_Children_Shelter').alias('Family_with_Children_Shelter'))

# Read in and clean NYC Seasonal Employment data
employment = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[4])
employment = employment.drop('PUBLICATION_DATE', 'METHOD', 'REVISION_REASON')
employment = employment.withColumn('INDUSTRY', functions.regexp_replace('INDUSTRY', ' ', '_'))
employment = employment.withColumn('INDUSTRY', functions.regexp_replace('INDUSTRY', ',', ''))
employment = employment.withColumn('ref_date', functions.concat_ws('/', employment['REFERENCE_MONTH'],
                                                                   employment['REFERENCE_YEAR']))
employment = time_format(employment, 'ref_date', 'M/yyyy')
employment = employment.drop('REFERENCE_YEAR', 'REFERENCE_MONTH')
employment = employment.fillna(0)
pivot_employment = employment.groupby('ref_date').pivot('INDUSTRY').avg('EMPLOYMENT')
pivot_employment = pivot_employment.drop(
    'Accommodation_and_Food_Services',
    'Administrative_and_Waste_Services', 'Arts_Entertainment_and_Recreation',
    'Construction', 'Credit_Intermediation_and_Related_Activities', 'Educational_Services',
    'Employment_Services', 'Finance_and_Insurance', 'Health_Care_and_Social_Assistance',
    'Management_of_Companies_and_Enterprises', 'Natural_Resources_Mining_and_Construction',
    'Other_Services_except_public_administration',
    'Professional_Scientific_and_Technical_Services', 'Real_Estate_and_Rental_and_Leasing',
    'Securities_Commodity_Contracts_Investments', 'Utilities',
    'Securities_Commodity_Contracts_Investments_', 'Transportation_and_Warehousing'
)

# Read in and clean Dates data
dates = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[5])
dates = time_format(dates, 'date', 'yyyy/MM/dd')

# Joining COVID19 and DHS Datasets
covid.createOrReplaceTempView('covid')
dhs.createOrReplaceTempView('dhs')
dates.createOrReplaceTempView('dates')
cvd_dhs = spark.sql("SELECT DISTINCT * "
                    "FROM (SELECT * FROM dates D LEFT OUTER JOIN dhs DH on D.date = DH.Date_of_Census) DD "
                    "LEFT OUTER JOIN COVID C "
                    "ON C.DATE_OF_INTEREST = DD.date "
                    "ORDER BY DD.date")

cvd_dhs = cvd_dhs.select(
    'date', 'Total_Adults_in_Shelter', 'Total_Children_in_Shelter',
    'Total_Individuals_in_Shelter', 'Single_Adult_Men_in_Shelter',
    'Single_Adult_Women_in_Shelter', 'Total_Single_Adults_in_Shelter',
    'Families_with_Children_in_Shelter', 'Adults_in_Families_with_Children_in_Shelter',
    'Children_in_Families_with_Children_in_Shelter',
    "Total_Individuals_in_Families_with_Children_in_Shelter_", 'Adult_Families_in_Shelter',
    'Individuals_in_Adult_Families_in_Shelter', 'case_count', 'cum_case_count', 'hosp_count', 'cum_hosp_count',
    'death_count', 'death_count_prob', 'cum_death_count')

# Clean transformed datasets
cols = ['Total_Adults_in_Shelter', 'Total_Children_in_Shelter', 'Total_Individuals_in_Shelter']
cvd_dhs = clean_na(cvd_dhs, cols)

# Compute rolling averages of homeless sheltler census counts to replace null values
window2 = Window.partitionBy().orderBy('date').rowsBetween(Window.currentRow - 8, Window.currentRow - 1)
cvd_dhs = cvd_dhs.withColumn('total_adults_rolling_avg',
                             functions.mean(cvd_dhs['Total_Adults_in_Shelter']).over(window2))
cvd_dhs = cvd_dhs.withColumn('total_children_rolling_avg',
                             functions.mean(cvd_dhs['Total_Children_in_Shelter']).over(window2))
cvd_dhs = cvd_dhs.withColumn('total_individuals_rolling_avg',
                             functions.mean(cvd_dhs['Total_Individuals_in_Shelter']).over(window2))

# Computing p-scores for Adults and Children in homeless shelters
cvd_dhs.createOrReplaceTempView('cvd_dhs')
result = spark.sql("SELECT CD.date, CD.case_count, CD.cum_case_count, CD.hosp_count, "
                   "CD.cum_hosp_count, CD.death_count, CD.death_count_prob, CD.cum_death_count, "
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
                   "GROUP BY CD.date, CD.case_count, CD.cum_case_count, CD.hosp_count, "
                   "CD.cum_hosp_count, CD.death_count, CD.death_count_prob, CD.cum_death_count, "
                   "adults_cvd, children_cvd, total_cvd "
                   "ORDER BY CD.date")

result = result.filter(result.date < functions.lit('2020-11-23'))

# Save output to csv to use for visualization
result.write.csv('excess_homelessness.csv')

# Join COVID and DHS Census by Borough data
covid.createOrReplaceTempView('covid')
pivot_dhs_borough.createOrReplaceTempView('dhs')
dates.createOrReplaceTempView('dates')

cvd_dhs_borough = spark.sql("SELECT DISTINCT DD.date, C.*, bx_shelter_pop, bk_shelter_pop, mn_shelter_pop, "
                            "qn_shelter_pop, si_shelter_pop "
                            "FROM (SELECT D.date, "
                            "(Bronx_Adult_Family_Commercial_Hotel + Bronx_Adult_Family_Shelter + Bronx_Adult_Shelter "
                            "+ Bronx_Adult_Shelter_Commercial_Hotel + Bronx_Family_Cluster "
                            "+ Bronx_Family_with_Children_Commercial_Hotel "
                            "+ Bronx_Family_with_Children_Shelter) as bx_shelter_pop,  "
                            "(Brooklyn_Adult_Family_Commercial_Hotel + Brooklyn_Adult_Family_Shelter "
                            "+ Brooklyn_Adult_Shelter + Brooklyn_Adult_Shelter_Commercial_Hotel "
                            "+ Brooklyn_Family_Cluster "
                            "+ Brooklyn_Family_with_Children_Commercial_Hotel "
                            "+ Brooklyn_Family_with_Children_Shelter) as bk_shelter_pop, "
                            "(Manhattan_Adult_Family_Commercial_Hotel + Manhattan_Adult_Family_Shelter "
                            "+ Manhattan_Adult_Shelter + Manhattan_Adult_Shelter_Commercial_Hotel "
                            "+ Manhattan_Family_Cluster "
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

# Clean transformed datasets
cols = ['bx_shelter_pop', 'bk_shelter_pop', 'mn_shelter_pop', 'qn_shelter_pop', 'si_shelter_pop', 'case_count',
        'hosp_count', 'death_count', 'death_count_prob', 'bx_case_count', 'bx_hosp_count', 'bx_death_count',
        'bk_case_count', 'bk_hosp_count', 'bk_death_count', 'mn_case_count', 'mn_hosp_count', 'mn_death_count',
        'qn_case_count', 'qn_hosp_count', 'qn_death_count', 'si_case_count', 'si_hosp_count', 'si_death_count']
cvd_dhs_borough = col_cast(cvd_dhs_borough, cols, types.IntegerType())

cvd_dhs_borough = cvd_dhs_borough.fillna(0, subset=['case_count', 'hosp_count', 'death_count', 'death_count_prob',
                                                    'bx_case_count', 'bx_hosp_count', 'bx_death_count',
                                                    'bk_case_count', 'bk_hosp_count', 'bk_death_count',
                                                    'mn_case_count', 'mn_hosp_count', 'mn_death_count',
                                                    'qn_case_count', 'qn_hosp_count', 'qn_death_count',
                                                    'si_case_count', 'si_hosp_count', 'si_death_count'])

cols = ['bx_shelter_pop', 'bk_shelter_pop', 'mn_shelter_pop', 'qn_shelter_pop', 'si_shelter_pop']
cvd_dhs_borough = clean_na(cvd_dhs_borough, cols)

# Compute cumulative counts by borough and rolling averages of DHS census data
window = Window.partitionBy().orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow)
cols = ['bx_case_count', 'bx_hosp_count', 'bx_death_count',
        'bk_case_count', 'bk_hosp_count', 'bk_death_count',
        'mn_case_count', 'mn_hosp_count', 'mn_death_count',
        'qn_case_count', 'qn_hosp_count', 'qn_death_count',
        'si_case_count', 'si_hosp_count', 'si_death_count']
cvd_dhs_borough = cum_count(cvd_dhs_borough, cols, window)

window2 = Window.partitionBy().orderBy('date').rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)
cols = ['bx_shelter_pop', 'bk_shelter_pop', 'mn_shelter_pop', 'qn_shelter_pop', 'si_shelter_pop']
add = '_pop_rolling_avg'
for c in cols:
    cvd_dhs_borough = cvd_dhs_borough.withColumn(c.split("_", 1)[0] + add,
                                                 functions.mean(cvd_dhs_borough[c]).over(window2))

# Compute p-scores for each borough
cvd_dhs_borough.createOrReplaceTempView('cvd_dhs')
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
result = result.filter(result.date == functions.last_day(result.date))

# Save output to csv
result.write.csv('excess_homelessness_borough.csv')

# Join COVID and NYC Seasonal Employment Data
covid.createOrReplaceTempView('covid')
pivot_employment.createOrReplaceTempView('employment')
dates.createOrReplaceTempView('dates')

cvd_labor = spark.sql("SELECT DISTINCT D.date, C1.cum_case_count, C1.cum_hosp_count, C1.cum_death_count, E.* "
                      "FROM (SELECT * FROM dates WHERE DAY(dates.date) = 1) D "
                      "LEFT OUTER JOIN employment E ON D.date = E.ref_date "
                      "LEFT OUTER JOIN covid C1 "
                      "ON (MONTH(D.date) = MONTH(C1.DATE_OF_INTEREST) "
                      "AND C1.DATE_OF_INTEREST = LAST_DAY(D.date)) "
                      "ORDER BY D.date")

cvd_labor = cvd_labor.drop('ref_date')

# Compute p-scores for each industry included
cvd_labor.createOrReplaceTempView('cvd_labor')
result = spark.sql("SELECT C1.date, C1.cum_case_count, C1.cum_hosp_count, C1.cum_death_count, "
                   "AVG(C3.Total_Nonfarm) as non_farm_avg, "
                   "AVG(C3.Total_Private) as private_avg, AVG(C3.Education_and_Health_Services) as ed_health_avg, "
                   "AVG(C3.Professional_and_Business_Services) as prof_bus_avg, "
                   "AVG(C3.Trade_Transportation_and_Utilities) as ttu_avg, "
                   "AVG(C3.Public_Administration) as pub_admin_avg, "
                   "AVG(C3.Financial_Activities) as fin_avg, AVG(C3.Leisure_and_Hospitality) as hosp_avg, "
                   "AVG(C3.Retail_Trade) as retail_avg, AVG(C3.Wholesale_Trade) as wholesale_avg, "
                   "AVG(C3.Manufacturing) as mfg_avg, AVG(C3.Information) as info_avg, "
                   "C1.Total_Nonfarm as non_farm_cvd, "
                   "C1.Total_Private as private_cvd, C1.Education_and_Health_Services as ed_health_cvd, "
                   "C1.Professional_and_Business_Services as prof_bus_cvd, "
                   "C1.Trade_Transportation_and_Utilities as ttu_cvd, C1.Public_Administration as pub_admin_cvd, "
                   "C1.Financial_Activities as fin_cvd, C1.Leisure_and_Hospitality as hosp_cvd, "
                   "C1.Retail_Trade as retail_cvd, C1.Wholesale_Trade as wholesale_cvd, "
                   "C1.Manufacturing as mfg_cvd, C1.Information as info_cvd, "
                   "((C1.Total_Nonfarm - AVG(C3.Total_Nonfarm)) / AVG(C3.Total_Nonfarm)) as non_farm_pscore, "
                   "((C1.Total_Private - AVG(C3.Total_Private)) / AVG(C3.Total_Private)) as private_pscore, "
                   "((C1.Education_and_Health_Services - AVG(C3.Education_and_Health_Services)) "
                   "/ AVG(C3.Education_and_Health_Services)) as ed_health_pscore, "
                   "((C1.Professional_and_Business_Services - AVG(C3.Professional_and_Business_Services)) "
                   "/ AVG(C3.Professional_and_Business_Services)) as prof_bus_pscore, "
                   "((C1.Trade_Transportation_and_Utilities - AVG(C3.Trade_Transportation_and_Utilities)) "
                   "/ AVG(C3.Trade_Transportation_and_Utilities)) as ttu_pscore, "
                   "((C1.Public_Administration - AVG(C3.Public_Administration)) / "
                   "AVG(C3.Public_Administration)) as pub_admin_pscore, "
                   "((C1.Financial_Activities - AVG(C3.Financial_Activities)) / "
                   "AVG(C3.Financial_Activities)) as fin_pscore, "
                   "((C1.Leisure_and_Hospitality - AVG(C3.Leisure_and_Hospitality)) / "
                   "AVG(C3.Leisure_and_Hospitality)) as hosp_pscore, "
                   "((C1.Retail_Trade - AVG(C3.Retail_Trade)) / AVG(C3.Retail_Trade)) as retail_pscore, "
                   "((C1.Wholesale_Trade - AVG(C3.Wholesale_Trade)) / AVG(C3.Wholesale_Trade)) as wholesale_pscore, "
                   "((C1.Manufacturing - AVG(C3.Manufacturing)) / AVG(C3.Manufacturing)) as mfg_pscore, "
                   "((C1.Information - AVG(C3.Information)) / AVG(C3.Information)) as info_pscore "
                   "FROM cvd_labor C1 LEFT OUTER JOIN "
                   "(SELECT * FROM cvd_labor C2 WHERE YEAR(C2.date) >= 2010 AND YEAR(C2.date) < 2020) C3 "
                   "ON MONTH(C1.date) = MONTH(C3.date) "
                   "WHERE YEAR(C1.date) >= 2020 AND MONTH(C1.date) >= 2 "
                   "GROUP BY C1.date, C1.cum_case_count, C1.cum_hosp_count, C1.cum_death_count, "
                   "non_farm_cvd, private_cvd, ed_health_cvd, prof_bus_cvd, ttu_cvd, pub_admin_cvd, fin_cvd, "
                   "hosp_cvd, retail_cvd, wholesale_cvd, mfg_cvd, info_cvd "
                   "ORDER BY C1.date")


result = result.filter(result.date < functions.lit('2020-11-01'))

# Save output to csv
result.write.csv('excess_unemployment.csv')

spark.stop()
