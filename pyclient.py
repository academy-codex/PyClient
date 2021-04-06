import os
import signal
import sys

from py4j.java_gateway import JavaGateway, Py4JError, GatewayParameters

from py4j.java_gateway import JavaGateway, GatewayParameters, JVMView, java_import

from pyspark.sql.functions import udf, col, lit
from pyspark.sql import column
from pyspark.sql import DataFrame

from pyspark import SQLContext
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

SIMPLE_PYTHON_SCRIPT_1 = "a = 2"

SIMPLE_PYTHON_SCRIPT_2 = """l = gateway.jvm.java.util.ArrayList()
l.append("a")
l.append("b")
"""


PYSPARK = """
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

# gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25445), auto_convert=True)

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

#########################################################################################################

class PyTransformer(object):
    def invoke(dataFramesMap):
        table = dataFramesMap.get("df")
        df = sqlC.sql("SELECT * FROM "+table)
        outputDf = mainTransformLogic(df)
        outputDf.createOrReplaceTempView("df_out")

        output = {'df_output': "df_out"}
        return output

    class Java:
        implements = ['spark.control.PyJavaControlComm']

########################################################################################################

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

pyTransformer = PyTransformer()
gateway.entry_point.execute(pyTransformer)
"""

class PythonInterpreter(object):

    def __init__(self, gateway):
        self.gateway = gateway

    def callPython(self, dfMap):
        locals = {}
        globals = {"gateway": self.gateway}

        sql_context = self.initJySpark()
        df = sql_context.sql("SELECT * FROM "+dfMap.get("df1"))
        output = self.mainTransformLogic(df)

        output.createOrReplaceTempView("df_out")

        output_map = {}
        returnValue = None


        output_map["output1"] = "df_out"

        from py4j.java_collections import MapConverter
        returnValue = MapConverter().convert(output_map, self.gateway._gateway_client)

        return returnValue

    def mainTransformLogic(self, df):
        df_copy = df.cache()
        df_copy.count()

        df_copy.describe(gateway = self.gateway)

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

    def initJySpark(self):
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

        import_pyspark_libs(self.gateway)
        spark_utils = self.gateway.entry_point.getSparkUtils()

        sparkConf = SparkConf(_jconf=spark_utils.getSparkConf())
        # session = SparkSession.builder.master("local").appName("SimpleApplication").getOrCreate(gateway=gateway)
        sc = SparkContext(conf=sparkConf,jsc=spark_utils.getSparkContext(),gateway=self.gateway)
        # sc = session.sparkContext
        session = SparkSession(sc, jsparkSession=spark_utils.getSparkSession())
        sqlC = SQLContext(sc)

        return sqlC


    class Java:
        implements = ["spark.interpret.communication.PythonInterpreter"]

def main():
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=25445, auto_convert=True), start_callback_server=True)
    interpreter = PythonInterpreter(gateway)
    gateway.entry_point.setPythonInterpreter(interpreter)

    # gateway.entry_point.interpret(PYSPARK)

    # Will raise an exception because the Java side is closing the connection.
    # Something to polish a bit...
    try:
        # gateway.entry_point.runFromJava()
        # gateway.shutdown_callback_server()
        # gateway.close()
        print(os.getpid())
        # os.kill(os.getpid(), signal.SIGTERM)
        # sys.exit()
        # gateway.shutdown()
    except Py4JError:
        pass


if __name__ == "__main__":
    main()
    sys.exit()