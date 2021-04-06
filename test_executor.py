from py4j.java_gateway import JavaGateway, GatewayParameters, JVMView

from patch.functionsPatched import udf, col, lit
from pyspark.sql import column
from pyspark.sql import DataFrame

from patch.SqlContextPatched import SQLContext
from pyspark import SparkConf
from patch.SparkContextPatched import SparkContext
from patch.MySparkSession import SparkSession

print("hi")

gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25445))

originalGateway = JavaGateway(
    gateway_parameters=GatewayParameters(
        port=63450,
        auth_token='abcd',
        auto_convert=True))
proc = None

spark_utils = gateway.entry_point.getSparkUtils()

sparkConf = SparkConf(_jconf=spark_utils.getSparkConf())
sparkContext = SparkContext(jsc=spark_utils.getSparkContext(), gateway=originalGateway)
# sparkContext = SparkSession.builder.appName("test").master("local").getOrCreate().sparkContext
# sqlContext = SQLContext(sparkContext = spark_utils.getSparkContext())
sqlContext = SQLContext(sparkContext = sparkContext)

func = udf(lambda a: a+"ABCD")
input_df2 = sqlContext.sql("SELECT * FROM testTable")
input_df2.withColumn(colName='Country2', col = lit('X')).show()

def customFunction(row):
    return row.Country

input_df2.rdd.map(customFunction).count()

# input_df2.withColumn("t", func(input_df2['Country'])).show()