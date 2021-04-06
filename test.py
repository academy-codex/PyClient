from py4j.java_gateway import JavaGateway, GatewayParameters

from pyspark.sql.functions import udf
from pyspark.sql import column
from pyspark.sql import SparkSession, DataFrame, SQLContext
from pyspark import SparkContext

print("hi")

gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25445))


spark_utils = gateway.entry_point.getSparkUtils()
spark_utils.showDataFrame()

# spark = SparkSession \
#     .builder \
#     .appName("PySpark using Scala example") \
#     .getOrCreate()
#
# # SparkContext from the SparkSession
# sc = spark._sc
#
# # SQLContext instantiated with Java components
# sqlContext = spark._wrapped

jSqlContext = spark_utils.getSqlContext()
jSparkContext = spark_utils.getSparkContext()

def convertCase(str):
    resStr=""
    arr = str.split(" ")
    for x in arr:
        resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

convertUDF = udf(lambda z: convertCase(z))

javaDf = spark_utils.getTestDataFrame()
input = DataFrame(javaDf, jSqlContext)
javaDf.registerTempTable("test")
# input_df = DataFrame(javaDf, sqlContext)

x = 'govt'
# SPARK SQL
print("SQL")
jSqlContext.sql("SELECT * FROM test WHERE 'test.Product'=' Montana '").show()

# SHOW
print("DF.show()")
javaDf.show()

print("FILTER")
javaDf.filter("Segment=='govt'").show()

print("JOIN")
javaDf2 = spark_utils.getDf2()
javaDf.join(javaDf2, "Segment").show()
# spark.sql("SELECT * ")
# transformed_df = input_df.withColumn('UpperCase', convertUDF(input_df['Country']))

# transformed_df.show()

print("Sending output")
output = javaDf.filter("Segment=='{}'".format(x))

spark_utils.setOutputDf(output)

gateway.close()
