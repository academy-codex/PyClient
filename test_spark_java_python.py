# import findspark
# findspark.init()

from py4j.java_gateway import JavaGateway, GatewayParameters, JVMView, java_import

from pyspark.sql.functions import udf, col, lit
from pyspark.sql import column
from pyspark.sql import DataFrame

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

def import_pyspark_libs(spark_gateway):
    # Import the classes used by PySpark
    java_import(spark_gateway.jvm, "org.apache.spark.SparkConf")
    java_import(spark_gateway.jvm, "org.apache.spark.api.java.*")
    java_import(spark_gateway.jvm, "org.apache.spark.api.java.JavaSparkContext")
    java_import(spark_gateway.jvm, "org.apache.spark.api.python.*")
    java_import(spark_gateway.jvm, "org.apache.spark.ml.python.*")
    java_import(spark_gateway.jvm, "org.apache.spark.SparkContext")
    java_import(spark_gateway.jvm, "org.apache.spark.mllib.api.python.*")
    # TODO(davies): move into sql
    java_import(spark_gateway.jvm, "org.apache.spark.sql.*")
    java_import(spark_gateway.jvm, "org.apache.spark.sql.api.python.*")
    java_import(spark_gateway.jvm, "org.apache.spark.sql.hive.*")
    java_import(spark_gateway.jvm, "scala.Tuple2")

gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25445), auto_convert=True)

# originalGateway = JavaGateway(
#     gateway_parameters=GatewayParameters(
#         port=49521,
#         auto_convert=True))
# proc = None

import_pyspark_libs(gateway)

spark_utils = gateway.entry_point.getSparkUtils()

sparkConf = SparkConf(_jconf=spark_utils.getSparkConf())
# session = SparkSession.builder.master("local").appName("SimpleApplication").getOrCreate(gateway=gateway)
sc = SparkContext(conf=sparkConf,jsc=spark_utils.getSparkContext(),gateway=gateway)
# sc = session.sparkContext
session = SparkSession(sc, jsparkSession=spark_utils.getSparkSession())
sqlC = SQLContext(sc)
spark_utils.getTestDataFrame().createOrReplaceTempView("mytable")

# #########################################################################################################
#
# class PyTransformer(object):
#     def invoke(dataFramesMap):
#         table = dataFramesMap.get("df")
#         df = sqlC.sql("SELECT * FROM "+table)
#         outputDf = mainTransformLogic(df)
#         outputDf.createOrReplaceTempView("df_out")
#
#         output = {'df_output': "df_out"}
#         return output
#
#     class Java:
#         implements = ['spark.control.PyJavaControlComm']
#
# ########################################################################################################

def mainTransformLogic(df):
    df_copy = df.cache()
    df_copy.count()

    df_copy.describe(gateway = gateway)

    df_copy = df_copy.withColumn("X", lit('Y'))
    df_copy.show()

    func = udf(lambda a: a+"ABCD")

    df_copy = df_copy.withColumn("Z", func(df_copy['X']))
    df_copy.printSchema()

    def for_each_test(row):
        return row.Z

    print(df_copy.rdd.map(for_each_test).count())
    df_copy.show()

    return df_copy

# table = dataFramesMap.get("df")
df = sqlC.sql("SELECT * FROM "+"testTable")
outputDf = mainTransformLogic(df)
outputDf.createOrReplaceTempView("df_out")

gateway.entry_point.testControlReturn("df_out")

# pyTransformer = PyTransformer()
# gateway.entry_point.execute(pyTransformer)