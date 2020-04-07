from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import format_number
from pyspark.sql.functions import lower
from pyspark.sql.functions import year
import sys

input_path = sys.argv[1]
output_path = sys.argv[2]

if __name__ == '__main__':

	spark = SparkSession.builder.getOrCreate()

	# create a pyspark df
	complaints = spark.read.csv(input_path, header=True, multiLine=True, escape='"', inferSchema=True)
	# select relevant columns only
	complaints = complaints.select(complaints['Date received'].alias('Date'), lower(complaints['Product']).alias('Product'), complaints['Company'])
	# extract the year
	complaints = complaints.withColumn('Year', year(complaints['Date']))
	# Find Total Complaints by Product and Year
	results1 = complaints.groupBy('Product','Year').count().orderBy('Product','Year')
	results1 = results1.withColumnRenamed('count','Complaints')
	# Find number of companies that have at least one complaint by Product and Year
	companies = complaints.groupBy('Product','Year').agg(countDistinct('Company')).orderBy('Product','Year')
	companies = companies.withColumnRenamed('count(DISTINCT Company)','Unique_Companies')
	# Find the highest number of complaints received by one company by Product and Year
	percent = complaints.groupBy('Product','Year','Company').count().orderBy('Product','Year','Company')
	percent = percent.withColumnRenamed('count','Most_Complaints')
	percent = percent.groupBy('Product','Year').agg({"Most_Complaints":"max"}).orderBy('Product','Year')
	# join two pyspark dataframes: companies and complaints(results1) on columns 'Product' and 'Year'
	results = results1.join(companies, ['Product','Year'])
	# join two pyspark dataframes: results and percent on columns 'Product' and 'Year'
	results = results.join(percent, ['Product','Year'])
	results = results.withColumnRenamed('max(Most_Complaints)','Max_One_Company')
	# calculate highest percentage of total complainst filed against one company by product and year
	results = results.withColumn('Percent', format_number(results['Max_One_Company']*100/results['Complaints'],0))
	results = results.drop('Max_One_Company')
	
	results.sort('Product','Year')
	results.write.csv(output_path)
