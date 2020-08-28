from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp
from pyspark.sql.functions import from_unixtime
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from datetime import datetime
from time import time
import numpy as np
import pandas as pd
import boto3


spark = SparkSession.builder.appName("covid19_nova_etl").config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory").enableHiveSupport().getOrCreate()

df_population = spark.sql("""select sum(popestimate2019) as population, ctyname, stname 
from covid19.cdc_county_population 
where ( 
( lower(ctyname) like '%fairfax%' or lower(ctyname) like '%loudoun%' or lower(ctyname) like '%alexandria%' 
or lower(ctyname) like '%arlington%' ) and stname = 'Virginia'
) or 
( lower(ctyname) like '%district%' and stname = 'District of Columbia' ) or 
( lower(ctyname) like '%montgomery%' and stname = 'Maryland' )  
group by ctyname, stname
order by ctyname""")


df_population_fairfax_b4 = df_population.filter(df_population.ctyname.contains("Fairfax"))

population_fairfax = df_population_fairfax_b4.groupBy().sum().collect()[0][0]


df_population_pd = df_population.toPandas()
counties = ['Alexandria','Arlington','District of Columbia','Fairfax','Fairfax city','Loudoun','Montgomery']

df_population_pd.insert(2,"County", counties)
df_population_pd.at[3,'population'] = population_fairfax

df_population_pd.drop(df_population_pd[df_population_pd['ctyname']=='Fairfax city'].index, inplace=True)




print (df_population_pd.index[-1])
whereclause = ""

for row in df_population_pd.itertuples():
    #print(row)
    #print(row.stname)
    whereclause = whereclause + "(province_state = '" + row.stname + "' and admin2 = '" + row.County + "')"
    
    if row.Index < df_population_pd.index[-1]:
        whereclause = whereclause + " OR "




sql_counties = """select from_unixtime(unix_timestamp(concat(year,'-',month,'-',day),'yyyy-MM-dd')) as date, 
sum(confirmed) as numConfirmed, sum(deaths) as numDeaths,sum(recovered) as numRecovered,admin2,province_state, 
concat(admin2, ' - ', province_state) as county 
from covid19.jhu_csse_covid_19_daily_reports
where year='2020' and int(month) > 3
and country_region = 'US' and ( """

sql_counties = sql_counties + whereclause

sql_counties = sql_counties + """ ) group by unix_timestamp(concat(year,'-',month,'-',day),'yyyy-MM-dd'),admin2,province_state
order by unix_timestamp(concat(year,'-',month,'-',day),'yyyy-MM-dd') asc, sum(confirmed) desc"""




df_counties = spark.sql(sql_counties)
df_counties.show()

df_fairfax = df_counties.filter(df_counties.admin2 == "Fairfax")



population_fairfax = df_population_pd[df_population_pd['County']=='Fairfax']["population"].values[0]

df_fairfax = df_fairfax.withColumn("numConfirmedPerM",((df_fairfax.numConfirmed * 1000000)/int(population_fairfax)).cast(DecimalType(10,0)))
df_fairfax = df_fairfax.withColumn("numDeathsPerM",((df_fairfax.numDeaths * 1000000)/int(population_fairfax)).cast(DecimalType(10,0)))
df_fairfax = df_fairfax.withColumn("numRecoveredPerM",((df_fairfax.numRecovered * 1000000)/int(population_fairfax)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_fairfax = df_fairfax.withColumn("dayBefore", F.lag(df_fairfax.numConfirmed).over(my_window))
df_fairfax = df_fairfax.withColumn("change", F.when(F.isnull(df_fairfax.numConfirmed - df_fairfax.dayBefore), 0).otherwise(df_fairfax.numConfirmed - df_fairfax.dayBefore))

df_fairfax = df_fairfax.withColumn("dayBeforeDeaths", F.lag(df_fairfax.numDeaths).over(my_window))
df_fairfax = df_fairfax.withColumn("changeDeaths", F.when(F.isnull(df_fairfax.numDeaths - df_fairfax.dayBeforeDeaths), 0).otherwise(df_fairfax.numDeaths - df_fairfax.dayBeforeDeaths))

df_fairfax = df_fairfax.withColumn("dayBeforeRecovered", F.lag(df_fairfax.numRecovered).over(my_window))
df_fairfax = df_fairfax.withColumn("changeRecovered", F.when(F.isnull(df_fairfax.numRecovered - df_fairfax.dayBeforeRecovered), 0).otherwise(df_fairfax.numRecovered - df_fairfax.dayBeforeRecovered))

df_fairfax = df_fairfax.withColumn("dayBeforePerM", F.lag(df_fairfax.numConfirmedPerM).over(my_window))
df_fairfax = df_fairfax.withColumn("changePerM", F.when(F.isnull(df_fairfax.numConfirmedPerM - df_fairfax.dayBeforePerM), 0).otherwise(df_fairfax.numConfirmedPerM - df_fairfax.dayBeforePerM))

df_fairfax = df_fairfax.withColumn("dayBeforeDeathsPerM", F.lag(df_fairfax.numDeathsPerM).over(my_window))
df_fairfax = df_fairfax.withColumn("changeDeathsPerM", F.when(F.isnull(df_fairfax.numDeathsPerM - df_fairfax.dayBeforeDeathsPerM), 0).otherwise(df_fairfax.numDeathsPerM - df_fairfax.dayBeforeDeathsPerM))

df_fairfax = df_fairfax.withColumn("dayBeforeRecoveredPerM", F.lag(df_fairfax.numRecoveredPerM).over(my_window))
df_fairfax = df_fairfax.withColumn("changeRecoveredPerM", F.when(F.isnull(df_fairfax.numRecoveredPerM - df_fairfax.dayBeforeRecoveredPerM), 0).otherwise(df_fairfax.numRecoveredPerM - df_fairfax.dayBeforeRecoveredPerM))




df_alexandria = df_counties.filter(df_counties.admin2 == "Alexandria")

population_alexandria = df_population_pd[df_population_pd['County']=='Alexandria']["population"].values[0]

df_alexandria = df_alexandria.withColumn("numConfirmedPerM",((df_alexandria.numConfirmed * 1000000)/int(population_alexandria)).cast(DecimalType(10,0)))
df_alexandria = df_alexandria.withColumn("numDeathsPerM",((df_alexandria.numDeaths * 1000000)/int(population_alexandria)).cast(DecimalType(10,0)))
df_alexandria = df_alexandria.withColumn("numRecoveredPerM",((df_alexandria.numRecovered * 1000000)/int(population_alexandria)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_alexandria = df_alexandria.withColumn("dayBefore", F.lag(df_alexandria.numConfirmed).over(my_window))
df_alexandria = df_alexandria.withColumn("change", F.when(F.isnull(df_alexandria.numConfirmed - df_alexandria.dayBefore), 0).otherwise(df_alexandria.numConfirmed - df_alexandria.dayBefore))

df_alexandria = df_alexandria.withColumn("dayBeforeDeaths", F.lag(df_alexandria.numDeaths).over(my_window))
df_alexandria = df_alexandria.withColumn("changeDeaths", F.when(F.isnull(df_alexandria.numDeaths - df_alexandria.dayBeforeDeaths), 0).otherwise(df_alexandria.numDeaths - df_alexandria.dayBeforeDeaths))

df_alexandria = df_alexandria.withColumn("dayBeforeRecovered", F.lag(df_alexandria.numRecovered).over(my_window))
df_alexandria = df_alexandria.withColumn("changeRecovered", F.when(F.isnull(df_alexandria.numRecovered - df_alexandria.dayBeforeRecovered), 0).otherwise(df_alexandria.numRecovered - df_alexandria.dayBeforeRecovered))

df_alexandria = df_alexandria.withColumn("dayBeforePerM", F.lag(df_alexandria.numConfirmedPerM).over(my_window))
df_alexandria = df_alexandria.withColumn("changePerM", F.when(F.isnull(df_alexandria.numConfirmedPerM - df_alexandria.dayBeforePerM), 0).otherwise(df_alexandria.numConfirmedPerM - df_alexandria.dayBeforePerM))

df_alexandria = df_alexandria.withColumn("dayBeforeDeathsPerM", F.lag(df_alexandria.numDeathsPerM).over(my_window))
df_alexandria = df_alexandria.withColumn("changeDeathsPerM", F.when(F.isnull(df_alexandria.numDeathsPerM - df_alexandria.dayBeforeDeathsPerM), 0).otherwise(df_alexandria.numDeathsPerM - df_alexandria.dayBeforeDeathsPerM))

df_alexandria = df_alexandria.withColumn("dayBeforeRecoveredPerM", F.lag(df_alexandria.numRecoveredPerM).over(my_window))
df_alexandria = df_alexandria.withColumn("changeRecoveredPerM", F.when(F.isnull(df_alexandria.numRecoveredPerM - df_alexandria.dayBeforeRecoveredPerM), 0).otherwise(df_alexandria.numRecoveredPerM - df_alexandria.dayBeforeRecoveredPerM))



df_arlington = df_counties.filter(df_counties.admin2 == "Arlington")

population_arlington = df_population_pd[df_population_pd['County']=='Arlington']["population"].values[0]

df_arlington = df_arlington.withColumn("numConfirmedPerM",((df_arlington.numConfirmed * 1000000)/int(population_arlington)).cast(DecimalType(10,0)))
df_arlington = df_arlington.withColumn("numDeathsPerM",((df_arlington.numDeaths * 1000000)/int(population_arlington)).cast(DecimalType(10,0)))
df_arlington = df_arlington.withColumn("numRecoveredPerM",((df_arlington.numRecovered * 1000000)/int(population_arlington)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_arlington = df_arlington.withColumn("dayBefore", F.lag(df_arlington.numConfirmed).over(my_window))
df_arlington = df_arlington.withColumn("change", F.when(F.isnull(df_arlington.numConfirmed - df_arlington.dayBefore), 0).otherwise(df_arlington.numConfirmed - df_arlington.dayBefore))

df_arlington = df_arlington.withColumn("dayBeforeDeaths", F.lag(df_arlington.numDeaths).over(my_window))
df_arlington = df_arlington.withColumn("changeDeaths", F.when(F.isnull(df_arlington.numDeaths - df_arlington.dayBeforeDeaths), 0).otherwise(df_arlington.numDeaths - df_arlington.dayBeforeDeaths))

df_arlington = df_arlington.withColumn("dayBeforeRecovered", F.lag(df_arlington.numRecovered).over(my_window))
df_arlington = df_arlington.withColumn("changeRecovered", F.when(F.isnull(df_arlington.numRecovered - df_arlington.dayBeforeRecovered), 0).otherwise(df_arlington.numRecovered - df_arlington.dayBeforeRecovered))

df_arlington = df_arlington.withColumn("dayBeforePerM", F.lag(df_arlington.numConfirmedPerM).over(my_window))
df_arlington = df_arlington.withColumn("changePerM", F.when(F.isnull(df_arlington.numConfirmedPerM - df_arlington.dayBeforePerM), 0).otherwise(df_arlington.numConfirmedPerM - df_arlington.dayBeforePerM))

df_arlington = df_arlington.withColumn("dayBeforeDeathsPerM", F.lag(df_arlington.numDeathsPerM).over(my_window))
df_arlington = df_arlington.withColumn("changeDeathsPerM", F.when(F.isnull(df_arlington.numDeathsPerM - df_arlington.dayBeforeDeathsPerM), 0).otherwise(df_arlington.numDeathsPerM - df_arlington.dayBeforeDeathsPerM))

df_arlington = df_arlington.withColumn("dayBeforeRecoveredPerM", F.lag(df_arlington.numRecoveredPerM).over(my_window))
df_arlington = df_arlington.withColumn("changeRecoveredPerM", F.when(F.isnull(df_arlington.numRecoveredPerM - df_arlington.dayBeforeRecoveredPerM), 0).otherwise(df_arlington.numRecoveredPerM - df_arlington.dayBeforeRecoveredPerM))




df_dc = df_counties.filter(df_counties.admin2 == "District of Columbia")

population_dc = df_population_pd[df_population_pd['County']=='District of Columbia']["population"].values[0]

df_dc = df_dc.withColumn("numConfirmedPerM",((df_dc.numConfirmed * 1000000)/int(population_dc)).cast(DecimalType(10,0)))
df_dc = df_dc.withColumn("numDeathsPerM",((df_dc.numDeaths * 1000000)/int(population_dc)).cast(DecimalType(10,0)))
df_dc = df_dc.withColumn("numRecoveredPerM",((df_dc.numRecovered * 1000000)/int(population_dc)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_dc = df_dc.withColumn("dayBefore", F.lag(df_dc.numConfirmed).over(my_window))
df_dc = df_dc.withColumn("change", F.when(F.isnull(df_dc.numConfirmed - df_dc.dayBefore), 0).otherwise(df_dc.numConfirmed - df_dc.dayBefore))

df_dc = df_dc.withColumn("dayBeforeDeaths", F.lag(df_dc.numDeaths).over(my_window))
df_dc = df_dc.withColumn("changeDeaths", F.when(F.isnull(df_dc.numDeaths - df_dc.dayBeforeDeaths), 0).otherwise(df_dc.numDeaths - df_dc.dayBeforeDeaths))

df_dc = df_dc.withColumn("dayBeforeRecovered", F.lag(df_dc.numRecovered).over(my_window))
df_dc = df_dc.withColumn("changeRecovered", F.when(F.isnull(df_dc.numRecovered - df_dc.dayBeforeRecovered), 0).otherwise(df_dc.numRecovered - df_dc.dayBeforeRecovered))

df_dc = df_dc.withColumn("dayBeforePerM", F.lag(df_dc.numConfirmedPerM).over(my_window))
df_dc = df_dc.withColumn("changePerM", F.when(F.isnull(df_dc.numConfirmedPerM - df_dc.dayBeforePerM), 0).otherwise(df_dc.numConfirmedPerM - df_dc.dayBeforePerM))

df_dc = df_dc.withColumn("dayBeforeDeathsPerM", F.lag(df_dc.numDeathsPerM).over(my_window))
df_dc = df_dc.withColumn("changeDeathsPerM", F.when(F.isnull(df_dc.numDeathsPerM - df_dc.dayBeforeDeathsPerM), 0).otherwise(df_dc.numDeathsPerM - df_dc.dayBeforeDeathsPerM))

df_dc = df_dc.withColumn("dayBeforeRecoveredPerM", F.lag(df_dc.numRecoveredPerM).over(my_window))
df_dc = df_dc.withColumn("changeRecoveredPerM", F.when(F.isnull(df_dc.numRecoveredPerM - df_dc.dayBeforeRecoveredPerM), 0).otherwise(df_dc.numRecoveredPerM - df_dc.dayBeforeRecoveredPerM))



df_loudoun = df_counties.filter(df_counties.admin2 == "Loudoun")

population_loudoun = df_population_pd[df_population_pd['County']=='Loudoun']["population"].values[0]

df_loudoun = df_loudoun.withColumn("numConfirmedPerM",((df_loudoun.numConfirmed * 1000000)/int(population_loudoun)).cast(DecimalType(10,0)))
df_loudoun = df_loudoun.withColumn("numDeathsPerM",((df_loudoun.numDeaths * 1000000)/int(population_loudoun)).cast(DecimalType(10,0)))
df_loudoun = df_loudoun.withColumn("numRecoveredPerM",((df_loudoun.numRecovered * 1000000)/int(population_loudoun)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_loudoun = df_loudoun.withColumn("dayBefore", F.lag(df_loudoun.numConfirmed).over(my_window))
df_loudoun = df_loudoun.withColumn("change", F.when(F.isnull(df_loudoun.numConfirmed - df_loudoun.dayBefore), 0).otherwise(df_loudoun.numConfirmed - df_loudoun.dayBefore))

df_loudoun = df_loudoun.withColumn("dayBeforeDeaths", F.lag(df_loudoun.numDeaths).over(my_window))
df_loudoun = df_loudoun.withColumn("changeDeaths", F.when(F.isnull(df_loudoun.numDeaths - df_loudoun.dayBeforeDeaths), 0).otherwise(df_loudoun.numDeaths - df_loudoun.dayBeforeDeaths))

df_loudoun = df_loudoun.withColumn("dayBeforeRecovered", F.lag(df_loudoun.numRecovered).over(my_window))
df_loudoun = df_loudoun.withColumn("changeRecovered", F.when(F.isnull(df_loudoun.numRecovered - df_loudoun.dayBeforeRecovered), 0).otherwise(df_loudoun.numRecovered - df_loudoun.dayBeforeRecovered))

df_loudoun = df_loudoun.withColumn("dayBeforePerM", F.lag(df_loudoun.numConfirmedPerM).over(my_window))
df_loudoun = df_loudoun.withColumn("changePerM", F.when(F.isnull(df_loudoun.numConfirmedPerM - df_loudoun.dayBeforePerM), 0).otherwise(df_loudoun.numConfirmedPerM - df_loudoun.dayBeforePerM))

df_loudoun = df_loudoun.withColumn("dayBeforeDeathsPerM", F.lag(df_loudoun.numDeathsPerM).over(my_window))
df_loudoun = df_loudoun.withColumn("changeDeathsPerM", F.when(F.isnull(df_loudoun.numDeathsPerM - df_loudoun.dayBeforeDeathsPerM), 0).otherwise(df_loudoun.numDeathsPerM - df_loudoun.dayBeforeDeathsPerM))

df_loudoun = df_loudoun.withColumn("dayBeforeRecoveredPerM", F.lag(df_loudoun.numRecoveredPerM).over(my_window))
df_loudoun = df_loudoun.withColumn("changeRecoveredPerM", F.when(F.isnull(df_loudoun.numRecoveredPerM - df_loudoun.dayBeforeRecoveredPerM), 0).otherwise(df_loudoun.numRecoveredPerM - df_loudoun.dayBeforeRecoveredPerM))


df_montgomery = df_counties.filter(df_counties.admin2 == "Montgomery")

populationmontgomery = df_population_pd[df_population_pd['County']=='Montgomery']["population"].values[0]

df_montgomery = df_montgomery.withColumn("numConfirmedPerM",((df_montgomery.numConfirmed * 1000000)/int(populationmontgomery)).cast(DecimalType(10,0)))
df_montgomery = df_montgomery.withColumn("numDeathsPerM",((df_montgomery.numDeaths * 1000000)/int(populationmontgomery)).cast(DecimalType(10,0)))
df_montgomery = df_montgomery.withColumn("numRecoveredPerM",((df_montgomery.numRecovered * 1000000)/int(populationmontgomery)).cast(DecimalType(10,0)))


my_window = Window.partitionBy().orderBy("date")

df_montgomery = df_montgomery.withColumn("dayBefore", F.lag(df_montgomery.numConfirmed).over(my_window))
df_montgomery = df_montgomery.withColumn("change", F.when(F.isnull(df_montgomery.numConfirmed - df_montgomery.dayBefore), 0).otherwise(df_montgomery.numConfirmed - df_montgomery.dayBefore))

df_montgomery = df_montgomery.withColumn("dayBeforeDeaths", F.lag(df_montgomery.numDeaths).over(my_window))
df_montgomery = df_montgomery.withColumn("changeDeaths", F.when(F.isnull(df_montgomery.numDeaths - df_montgomery.dayBeforeDeaths), 0).otherwise(df_montgomery.numDeaths - df_montgomery.dayBeforeDeaths))

df_montgomery = df_montgomery.withColumn("dayBeforeRecovered", F.lag(df_montgomery.numRecovered).over(my_window))
df_montgomery = df_montgomery.withColumn("changeRecovered", F.when(F.isnull(df_montgomery.numRecovered - df_montgomery.dayBeforeRecovered), 0).otherwise(df_montgomery.numRecovered - df_montgomery.dayBeforeRecovered))

df_montgomery = df_montgomery.withColumn("dayBeforePerM", F.lag(df_montgomery.numConfirmedPerM).over(my_window))
df_montgomery = df_montgomery.withColumn("changePerM", F.when(F.isnull(df_montgomery.numConfirmedPerM - df_montgomery.dayBeforePerM), 0).otherwise(df_montgomery.numConfirmedPerM - df_montgomery.dayBeforePerM))

df_montgomery = df_montgomery.withColumn("dayBeforeDeathsPerM", F.lag(df_montgomery.numDeathsPerM).over(my_window))
df_montgomery = df_montgomery.withColumn("changeDeathsPerM", F.when(F.isnull(df_montgomery.numDeathsPerM - df_montgomery.dayBeforeDeathsPerM), 0).otherwise(df_montgomery.numDeathsPerM - df_montgomery.dayBeforeDeathsPerM))

df_montgomery = df_montgomery.withColumn("dayBeforeRecoveredPerM", F.lag(df_montgomery.numRecoveredPerM).over(my_window))
df_montgomery = df_montgomery.withColumn("changeRecoveredPerM", F.when(F.isnull(df_montgomery.numRecoveredPerM - df_montgomery.dayBeforeRecoveredPerM), 0).otherwise(df_montgomery.numRecoveredPerM - df_montgomery.dayBeforeRecoveredPerM))


#df_montgomery.show()

df_fairfax_p = df_fairfax.toPandas()
df_alexandria_p = df_alexandria.toPandas()
df_arlington_p = df_arlington.toPandas()
df_loudoun_p = df_loudoun.toPandas()
df_montgomery_p = df_montgomery.toPandas()
df_dc_p = df_dc.toPandas()

df_dmv = df_fairfax.union(df_arlington).union(df_montgomery).union(df_dc).union(df_loudoun).union(df_alexandria)
movAvgSpec = Window.partitionBy("county").orderBy("date").rowsBetween(-7,0)

df_dmv_movAvg_seven_wodr = df_dmv.withColumn( "movingAveragePerM", F.avg("changePerM").over(movAvgSpec) )
df_dmv_movAvg_seven_wor = df_dmv_movAvg_seven_wodr.withColumn( "movingAverageDeathsPerM", F.avg("changeDeathsPerM").over(movAvgSpec) ) 
df_dmv_movAvg_seven = df_dmv_movAvg_seven_wor.withColumn( "movingRecoveredPerM", F.avg("changeRecoveredPerM").over(movAvgSpec) ) 

curr_date = datetime.now()
day = str(curr_date.day).rjust(2, '0')
month = str(curr_date.month).rjust(2, '0')
year = str(curr_date.year)
hour = str(curr_date.hour).rjust(2,'0')
minute=str(curr_date.minute).rjust(2,'0')

dynamodate = year+'-'+month+'-'+day
dynamotime = hour+minute

database_table = 'dmv_moving_average_' + year+month+day + '_' + hour+minute

df_dmv_movAvg_seven.write.format('jdbc').options(
     url='jdbc:mysql://<mysql db server hostname>/<database name>',
     driver='com.mysql.jdbc.Driver',
     dbtable=database_table,
     user='',
     password='').mode('append').save()

client = boto3.client('dynamodb','us-east-2')
response = client.put_item(
    Item={
        'date': {
            'S': dynamodate,
        },
        'time': {
            'S': dynamotime,
        },
        'dbtable': {
            'S': database_table,
        },
    },
    TableName='covid19_etl_mysql',
)
