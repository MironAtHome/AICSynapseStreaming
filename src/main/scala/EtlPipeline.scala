package com.microsoft.azure.poc

import com.google.gson.GsonBuilder
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.microsoft.hyperspace._
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.CallableStatement

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement
import com.microsoft.sqlserver.jdbc.SQLServerConnection
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.types.{ArrayType, BinaryType, CalendarIntervalType, DateType, TimestampType, BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType, DecimalType, DoubleType, NumericType, CharType, StringType, VarcharType, HiveStringType, MapType, NullType, ObjectType}
import org.apache.spark.sql.functions.{col, to_date, to_timestamp, to_utc_timestamp}

class EtlPipeline(parentSpark :SparkSession) extends Thread {

    val pipelineSpark :SparkSession = parentSpark.newSession()
    import pipelineSpark.implicits._

    val hyperspace = new Hyperspace(pipelineSpark)

    val GSON = new GsonBuilder().create()

    val models:Models = new Models()

    override def run() {
      try {

        AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", "entered run method")

        val runIdList: Map[String,List[String]] = Map(
          "2020010104" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010104/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010104/"
          ))

          /*, "2020010112" -> List(
            "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010112/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010112/"
          ), "2020010120" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010120/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010120/"
          ), "2020010200" -> List(
            "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010200/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010200/"
          ), "2020010204" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010204/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010204/"
          ), "2020010208" -> List(
            "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010208/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010208/"
          ), "2020010216" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010216/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010216/"
          ), "2020010300" -> List(
            "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010300/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010300/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010300/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010300/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010300/"
          ), "2020010304" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010304/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010304/"
          ), "2020010308" -> List(
            "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010308/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010308/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010308/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010308/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010308/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010308/"
          ), "2020010312" -> List(
            "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010312/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010312/"
          ), "2020010316" -> List(
            "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe00.blob.core.windows.net/test48/Slice=7/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe22.blob.core.windows.net/test48/Slice=5/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010316/"
            , "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010316/"
          ), "2020010320" -> List(
            "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe23.blob.core.windows.net/test48/Slice=6/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe04.blob.core.windows.net/test48/Slice=11/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe02.blob.core.windows.net/test48/Slice=9/RunId=2020010320/"
            , "wasb://ucddv2@aciinteastusppe03.blob.core.windows.net/test48/Slice=10/RunId=2020010320/"
          ), "2020010400" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010400/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010400/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010400/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010400/"
          ), "2020010404" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010404/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010404/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010404/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010404/"
          ), "2020010408" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010408/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010408/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010408/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010408/"
          ), "2020010412" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010412/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010412/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010412/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010412/"
          ), "2020010416" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010416/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010416/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010416/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010416/"
          ), "2020010420" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010420/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010420/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010420/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010420/"
          ), "2020010500" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010500/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010500/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010500/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010500/"
          ), "2020010504" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010504/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010504/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010504/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010504/"
          ), "2020010508" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010508/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010508/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010508/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010508/"
          ), "2020010512" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010512/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010512/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010512/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010512/"
          ), "2020010516" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010516/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010516/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010516/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010516/"
          ), "2020010520" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010520/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010520/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010520/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010520/"
          ), "2020010600" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010600/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010600/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010600/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010600/"
          ), "2020010604" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010604/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010604/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010604/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010604/"
          ), "2020010608" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010608/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010608/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010608/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010608/"
          ), "2020010612" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010612/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010612/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010612/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010612/"
          ), "2020010616" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010616/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010616/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010616/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010616/"
          ), "2020010620" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010620/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010620/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010620/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010620/"
          ), "2020010700" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010700/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010700/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010700/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010700/"
          ), "2020010704" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010704/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010704/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010704/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010704/"
          ), "2020010708" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010708/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010708/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010708/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010708/"
          ), "2020010712" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010712/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010712/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010712/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010712/"
          ), "2020010716" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010716/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010716/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010716/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010716/"
          ), "2020010720" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010720/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010720/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010720/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010720/"
          ), "2020010800" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010800/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010800/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010800/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010800/"
          ), "2020010804" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010804/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010804/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010804/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010804/"
          ), "2020010808" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010808/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010808/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010808/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010808/"
          ), "2020010812" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010812/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010812/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010812/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010812/"
          ), "2020010816" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010816/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010816/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010816/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010816/"
          ), "2020010820" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010820/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010820/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010820/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010820/"
          ), "2020010900" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010900/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010900/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010900/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010900/"
          ), "2020010904" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010904/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010904/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010904/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010904/"
          ), "2020010908" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010908/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010908/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010908/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010908/"
          ), "2020010912" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010912/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010912/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010912/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010912/"
          ), "2020010916" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010916/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010916/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010916/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010916/"
          ), "2020010920" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020010920/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020010920/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020010920/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020010920/"
          ), "2020011000" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011000/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011000/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011000/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011000/"
          ), "2020011004" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011004/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011004/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011004/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011004/"
          ), "2020011008" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011008/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011008/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011008/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011008/"
          ), "2020011012" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011012/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011012/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011012/"
          ), "2020011016" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011016/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011016/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011016/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011016/"
          ), "2020011020" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011020/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011020/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011020/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011020/"
          ), "2020011100" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011100/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011100/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011100/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011100/"
          ), "2020011104" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011104/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011104/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011104/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011104/"
          ), "2020011108" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011108/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011108/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011108/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011108/"
          ), "2020011112" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011112/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011112/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011112/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011112/"
          ), "2020011116" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011116/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011116/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011116/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011116/"
          ), "2020011120" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011120/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011120/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011120/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011120/"
          ), "2020011200" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011200/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011200/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011200/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011200/"
          ), "2020011204" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011204/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011204/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011204/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011204/"
          ), "2020011208" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011208/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011208/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011208/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011208/"
          ), "2020011212" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011212/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011212/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011212/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011212/"
          ), "2020011216" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011216/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011216/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011216/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011216/"
          ), "2020011220" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011220/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011220/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011220/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011220/"
          ), "2020011300" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011300/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011300/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011300/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011300/"
          ), "2020011304" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011304/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011304/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011304/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011304/"
          ), "2020011308" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011308/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011308/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011308/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011308/"
          ), "2020011312" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011312/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011312/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011312/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011312/"
          ), "2020011316" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011316/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011316/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011316/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011316/"
          ), "2020011320" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011320/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011320/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011320/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011320/"
          ), "2020011400" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011400/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011400/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011400/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011400/"
          ), "2020011404" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011404/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011404/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011404/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011404/"
          ), "2020011408" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011408/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011408/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011408/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011408/"
          ), "2020011412" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011412/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011412/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011412/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011412/"
          ), "2020011416" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011416/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011416/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011416/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011416/"
          ), "2020011420" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011420/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011420/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011420/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011420/"
          ), "2020011500" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011500/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011500/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011500/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011500/"
          ), "2020011504" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011504/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011504/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011504/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011504/"
          ), "2020011508" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011508/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011508/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011508/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011508/"
          ), "2020011512" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011512/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011512/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011512/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011512/"
          ), "2020011516" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011516/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011516/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011516/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011516/"
          ), "2020011520" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011520/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011520/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011520/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011520/"
          ), "2020011600" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011600/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011600/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011600/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011600/"
          ), "2020011604" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011604/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011604/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011604/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011604/"
          ), "2020011608" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011608/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011608/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011608/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011608/"
          ), "2020011612" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011612/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011612/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011612/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011612/"
          ), "2020011616" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011616/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011616/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011616/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011616/"
          ), "2020011620" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011620/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011620/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011620/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011620/"
          ), "2020011700" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011700/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011700/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011700/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011700/"
          ), "2020011704" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011704/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011704/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011704/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011704/"
          ), "2020011708" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011708/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011708/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011708/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011708/"
          ), "2020011712" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011712/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011712/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011712/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011712/"
          ), "2020011716" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011716/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011716/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011716/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011716/"
          ), "2020011720" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011720/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011720/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011720/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011720/"
          ), "2020011800" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011800/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011800/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011800/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011800/"
          ), "2020011804" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011804/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011804/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011804/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011804/"
          ), "2020011808" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011808/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011808/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011808/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011808/"
          ), "2020011812" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011812/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011812/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011812/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011812/"
          ), "2020011816" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011816/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011816/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011816/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011816/"
          ), "2020011820" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011820/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011820/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011820/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011820/"
          ), "2020011900" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011900/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011900/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011900/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011900/"
          ), "2020011904" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011904/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011904/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011904/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011904/"
          ), "2020011908" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011908/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011908/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011908/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011908/"
          ), "2020011912" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011912/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011912/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011912/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011912/"
          ), "2020011916" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011916/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011916/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011916/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011916/"
          ), "2020011920" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020011920/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020011920/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020011920/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020011920/"
          ), "2020012000" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012000/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012000/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012000/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012000/"
          ), "2020012004" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012004/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012004/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012004/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012004/"
          ), "2020012008" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012008/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012008/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012008/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012008/"
          ), "2020012012" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012012/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012012/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012012/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012012/"
          ), "2020012016" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012016/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012016/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012016/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012016/"
          ), "2020012020" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012020/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012020/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012020/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012020/"
          ), "2020012100" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012100/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012100/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012100/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012100/"
          ), "2020012104" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012104/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012104/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012104/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012104/"
          ), "2020012108" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012108/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012108/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012108/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012108/"
          ), "2020012112" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012112/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012112/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012112/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012112/"
          ), "2020012116" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012116/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012116/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012116/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012116/"
          ), "2020012120" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012120/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012120/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012120/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012120/"
          ), "2020012200" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012200/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012200/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012200/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012200/"
          ), "2020012204" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012204/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012204/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012204/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012204/"
          ), "2020012208" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012208/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012208/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012208/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012208/"
          ), "2020012212" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012212/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012212/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012212/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012212/"
          ), "2020012216" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012216/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012216/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012216/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012216/"
          ), "2020012220" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012220/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012220/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012220/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012220/"
          ), "2020012300" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012300/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012300/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012300/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012300/"
          ), "2020012304" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012304/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012304/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012304/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012304/"
          ), "2020012308" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012308/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012308/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012308/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012308/"
          ), "2020012312" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012312/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012312/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012312/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012312/"
          ), "2020012316" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012316/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012316/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012316/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012316/"
          ), "2020012320" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012320/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012320/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012320/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012320/"
          ), "2020012400" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012400/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012400/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012400/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012400/"
          ), "2020012404" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012404/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012404/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012404/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012404/"
          ), "2020012408" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012408/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012408/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012408/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012408/"
          ), "2020012412" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012412/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012412/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012412/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012412/"
          ), "2020012416" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012416/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012416/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012416/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012416/"
          ), "2020012420" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012420/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012420/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012420/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012420/"
          ), "2020012500" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012500/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012500/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012500/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012500/"
          ), "2020012504" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012504/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012504/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012504/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012504/"
          ), "2020012508" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012508/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012508/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012508/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012508/"
          ), "2020012512" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012512/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012512/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012512/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012512/"
          ), "2020012516" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012516/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012516/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012516/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012516/"
          ), "2020012520" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012520/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012520/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012520/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012520/"
          ), "2020012600" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012600/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012600/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012600/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012600/"
          ), "2020012604" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012604/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012604/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012604/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012604/"
          ), "2020012608" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012608/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012608/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012608/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012608/"
          ), "2020012612" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012612/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012612/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012612/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012612/"
          ), "2020012616" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012616/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012616/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012616/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012616/"
          ), "2020012620" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012620/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012620/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012620/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012620/"
          ), "2020012700" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012700/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012700/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012700/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012700/"
          ), "2020012704" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012704/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012704/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012704/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012704/"
          ), "2020012708" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012708/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012708/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012708/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012708/"
          ), "2020012712" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012712/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012712/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012712/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012712/"
          ), "2020012716" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012716/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012716/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012716/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012716/"
          ), "2020012720" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012720/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012720/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012720/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012720/"
          ), "2020012800" -> List(
            "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012800/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012800/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012800/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012800/"
          ), "2020012804" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012804/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012804/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012804/"
          ), "2020012808" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012808/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012808/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012808/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012808/"
          ), "2020012812" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012812/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012812/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012812/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012812/"
          ), "2020012816" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012816/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012816/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012816/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012816/"
          ), "2020012820" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012820/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012820/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012820/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012820/"
          ), "2020012900" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012900/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012900/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012900/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012900/"
          ), "2020012904" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012904/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012904/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012904/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012904/"
          ), "2020012908" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012908/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012908/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012908/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012908/"
          ), "2020012912" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012912/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012912/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012912/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012912/"
          ), "2020012916" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012916/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012916/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012916/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012916/"
          ), "2020012920" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020012920/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020012920/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020012920/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020012920/"
          ), "2020013000" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013000/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013000/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013000/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013000/"
          ), "2020013004" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013004/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013004/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013004/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013004/"
          ), "2020013008" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013008/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013008/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013008/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013008/"
          ), "2020013012" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013012/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013012/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013012/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013012/"
          ), "2020013016" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013016/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013016/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013016/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013016/"
          ), "2020013020" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013020/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013020/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013020/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013020/"
          ), "2020013100" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013100/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013100/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013100/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013100/"
          ), "2020013104" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013104/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013104/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013104/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013104/"
          ), "2020013108" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013108/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013108/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013108/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013108/"
          ), "2020013112" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013112/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013112/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013112/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013112/"
          ), "2020013116" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013116/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013116/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013116/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013116/"
          ), "2020013120" -> List(
            "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020013120/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020013120/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020013120/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020013120/"
          ), "2020020100" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020020100/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020020100/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020020100/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020020100/"
          ), "2020020104" -> List(
            "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020020104/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020020104/"
            , "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020020104/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020020104/"
          ), "2020020108" -> List(
            "wasb://ucddv2@aciinteastusppe21.blob.core.windows.net/test48/Slice=4/RunId=2020020108/"
            , "wasb://ucddv2@aciinteastusppe18.blob.core.windows.net/test48/Slice=1/RunId=2020020108/"
            , "wasb://ucddv2@aciinteastusppe19.blob.core.windows.net/test48/Slice=2/RunId=2020020108/"
            , "wasb://ucddv2@aciinteastusppe20.blob.core.windows.net/test48/Slice=3/RunId=2020020108/")) */

        var loadId:Long = 0
        val dfArray:Array[DataFrame] = new Array[DataFrame](2)

        runIdList.keys.foreach(key => {
          val currentLoadIndex:Int = (loadId % 2) .asInstanceOf[Int]
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", s"About to load slice ${key}")
          // "wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010316"
          val ingestionArgs = runIdList(key).toArray
          dfArray(currentLoadIndex) = ingestNewRunId(ingestionArgs:_*)
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", s"Loaded slice ${key}")
          dfArray(currentLoadIndex) = onResetView(dfArray(currentLoadIndex), key)
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", s"View ${MessageStreamingPipeline.viewName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} is created")
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", "View is ready to service data. Please send queue messages")

          val currentActiveExecutors = CurrentActiveExecutors(pipelineSpark.sparkContext)
          val currentActiveExecutorsJson = GSON.toJson(currentActiveExecutors)
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", s"Executor state: ${currentActiveExecutorsJson}")
          Thread.sleep(1200000)
          loadId += 1
        })

      } catch {
        case ex:Throwable => {
          val message = s"Exception in EtlPipeline.run of type ${ex.getClass().getSimpleName()}. Error message: ${ex.getMessage()}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", message)
          throw new SparkException("Exception in EtlPipeline.run", ex)
        }
      } finally {
        AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.run", "Exiting")
        //pipelineSpark.close()
      }
    }

    def ingestNewRunId(fileUrls :String*) : DataFrame = {
      //var df = pipelineSpark.read.schema(models.ucddDFSchemaGet()).json("wasb://ucddv2@aciinteastusppe01.blob.core.windows.net/test48/Slice=8/RunId=2020010316/part-00000-5f74f520-0f98-49d9-8361-6093a438b3cb.json.gz")
       pipelineSpark
        .read
        .schema(models.ucddDFSchemaGet())
        .json(fileUrls:_*)
        .withColumn("AccountId",col("AccountId").cast(LongType))
        .withColumn("BCToUSDExchangeRate",col("BCToUSDExchangeRate").cast(DoubleType))
        .withColumn("BillableItemId",col("BillableItemId").cast(LongType))
        .withColumn("BillingMonth",col("BillingMonth").cast(LongType))
        .withColumn("BillingMonthdt",to_date(to_timestamp(col("BillingMonthdt"))))
        .withColumn("ConsumedServiceId",col("ConsumedServiceId").cast(IntegerType))
        .withColumn("DepartmentId",col("DepartmentId").cast(LongType))
        .withColumn("EnrollmentId",col("EnrollmentId").cast(LongType))
        .withColumn("MeterId",col("MeterId").cast(LongType))
        .withColumn("PartnerId",col("PartnerId").cast(LongType))
        .withColumn("ResourceLocationId",col("ResourceLocationId").cast(IntegerType))
        .withColumn("SubscriptionId",col("SubscriptionId").cast(LongType))
        .withColumn("UnitPrice",col("UnitPrice").cast(DoubleType))
        .withColumn("UnitPriceUSD",col("UnitPriceUSD").cast(DoubleType))
        .withColumn("UnitPrice_Scaled",col("UnitPrice_Scaled").cast(DoubleType))
        .withColumn("UnitPrice_ScaledUSD",col("UnitPrice_ScaledUSD").cast(DoubleType))
        .withColumn("UsageDate",col("UsageDate").cast(IntegerType))
        .withColumn("UsageDatedt",to_date(to_timestamp(col("UsageDatedt"))))
        .withColumn("maxUsageTime",to_timestamp(col("UsageDatedt")))
        .withColumn("maxVortexIngestTime",to_timestamp(col("maxVortexIngestTime")))
        .withColumn("minUsageTime",to_timestamp(col("minUsageTime")))
        .withColumn("minVortexIngestTime",to_timestamp(col("minVortexIngestTime")))
        .repartition(158,$"SubscriptionId")
        .sortWithinPartitions($"maxVortexIngestTime")
    }

    def onNewRunId(fileUrl :String, cdf :DataFrame) : DataFrame = {
      pipelineSpark.sqlContext.read.json(fileUrl).union(cdf)
    }
    
    def onResetView(df :DataFrame, key:String) : DataFrame = {

      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", "Started")
      //val currentViewName = s"global_temp.${MessageStreamingPipeline.viewName.concat(RestfullStreamingPipeline.getCurrentPartitionIndex())}"
      val newViewIndex:String = MessageStreamingPipeline.getNextPartitionIndex(MessageStreamingPipeline.getCurrentPartitionIndex())
      val newViewName = s"${MessageStreamingPipeline.viewName.concat(newViewIndex)}"
      val newEnrSubResolverViewName = s"${MessageStreamingPipeline.viewEnrSubResolverName.concat(newViewIndex)}"
      val newAcctSubResolverViewName = s"${MessageStreamingPipeline.viewAcctSubResolverName.concat(newViewIndex)}"
      
      val newIndexName = s"${MessageStreamingPipeline.viewName}Index${newViewIndex}"
      val newIndexSubResNameEnr = s"${MessageStreamingPipeline.viewName}Index${newEnrSubResolverViewName}Enr"
      val newIndexSubResNameAcct = s"${MessageStreamingPipeline.viewName}Index${newAcctSubResolverViewName}Acct"
      
      df.write.format("parquet").mode("overwrite").save(s"/parquet/ucdddata/$key")
      val dfx = pipelineSpark.read.format("parquet").load(s"/parquet/ucdddata/$key")
      dfx.createOrReplaceGlobalTempView(newViewName)
      
      val subEnrResolverDF = pipelineSpark.sql(s"SELECT DISTINCT EnrollmentId, SubscriptionId FROM global_temp.${newViewName}")
      subEnrResolverDF.write.format("parquet").mode("overwrite").save(s"/parquet/ucdddata/enrsubres$key")
      val subEnrResolverDFx = pipelineSpark.read.format("parquet").load(s"/parquet/ucdddata/enrsubres$key")
      subEnrResolverDFx.createOrReplaceGlobalTempView(newEnrSubResolverViewName)

      val subAcctResolverDF = pipelineSpark.sql(s"SELECT DISTINCT AccountId, SubscriptionId FROM global_temp.${newViewName}")
      subAcctResolverDF.write.format("parquet").mode("overwrite").save(s"/parquet/ucdddata/acctsubres$key")
      val subAcctResolverDFx = pipelineSpark.read.format("parquet").load(s"/parquet/ucdddata/acctsubres$key")
      subAcctResolverDFx.createOrReplaceGlobalTempView(newAcctSubResolverViewName)
      
      // create hyperspace index
      val dfIndexConfig = IndexConfig(newIndexName, Seq("SubscriptionId"), Seq("EnrollmentId","AccountId","AdditionalInfo","BCToUSDExchangeRate","BillableItemId","BillingMonth", "BillingMonthdt", "ChannelType", "ConsumedService", "ConsumedServiceId", "DepartmentId", "EnrollmentNumber_Hie", "EnrollmentNumber_UST", "InstanceName", "IsMonetaryCommitmentService", "MeterId", "MeterResourceId","OfferId","PartnerId","ResourceGroup","ResourceLocationId","ResourceLocationNormalized","ResourceType","ServiceInfo1","ServiceInfo2","Sku", "SourceRegion","StoreServiceIdentifier","SubscriptionGuid","Tags","TotalCharges","USDTotalCharges","UnitPrice","UnitPriceUSD", "UnitPrice_Scaled","UnitPrice_ScaledUSD","UsageDate","UsageDatedt","maxUsageTime","maxVortexIngestTime","minUsageTime","minVortexIngestTime","resourceUnits","resourceUnits_Scaled"))
      
      val defIndexSubResEnrConfig = IndexConfig(newIndexSubResNameEnr, Seq("EnrollmentId"), Seq("SubscriptionId"))
      val defIndexSubResAcctConfig = IndexConfig(newIndexSubResNameAcct, Seq("AccountId"), Seq("SubscriptionId"))
      
      CreateIndexByDef(dfx, newIndexName, dfIndexConfig)
      CreateIndexByDef(subEnrResolverDFx, newIndexSubResNameEnr, defIndexSubResEnrConfig)
      CreateIndexByDef(subAcctResolverDFx, newIndexSubResNameAcct, defIndexSubResAcctConfig)
    
      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"Hyperspace index $newIndexName was created")
      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"View global_temp.${newViewName} was reset")
      //pipelineSpark.sql(s"CACHE TABLE global_temp.${newViewName}")
      //?? pipelineSpark.catalog.uncacheTable(currentViewName) onScheduleViewDrop(currentViewName)

      MessageStreamingPipeline.setNextPartitionIndex()
      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", "Completed")
      dfx
    }

    def CurrentActiveExecutors(sc: SparkContext): Seq[String] = {
      val allExecutors = sc.getExecutorMemoryStatus.map(_._1)
      val driverHost: String = sc.getConf.get("spark.driver.host")
      allExecutors.toList
    }

    def CreateIndexByDef(df :DataFrame, ixName :String,  ixConf :IndexConfig): Unit = {
        if(hyperspace.indexes.where($"name" === ixName).count == 1) {
        //name, indexedColumns, includedColumns, numBuckets, schema, indexLocation, queryPlan, state
        var indexHandle = hyperspace.indexes.where($"name" === ixName).take(1)(0)
        if(indexHandle.getAs[String](indexHandle.fieldIndex("state")).equals("CREATING")){
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"Hyperspace index $ixName is in CREATING state. Issuing cancel call.")
          hyperspace.cancel(ixName)
        }
        indexHandle = hyperspace.indexes.where($"name" === ixName).take(1)(0)
        if(indexHandle.getAs[String](indexHandle.fieldIndex("state")).equals("ACTIVE")){
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"Hyperspace index $ixName was found in a state of ACTIVE, deleting")
          hyperspace.deleteIndex(ixName)
        }
        indexHandle = hyperspace.indexes.where($"name" === ixName).take(1)(0)
        if(indexHandle.getAs[String](indexHandle.fieldIndex("state")).equals("DELETED")) {
          AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"Hyperspace index $ixName was found in state of DELETED. vacuuming")
          hyperspace.vacuumIndex(ixName)
        }
      }
      hyperspace.createIndex(df, ixConf)
      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onResetView", s"Hyperspace index $ixName created")
    }
//    def onScheduleViewDrop(viewName :String, pendingTaskList: Array[Future]) : Unit = {
//      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onScheduleViewDrop", "Started")
  //      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onScheduleViewDrop", s"View ${MessageStreamingPipeline.viewName.concat(newViewIndex)} was reset")
//      //pipelineSpark.catalog.uncacheTable(viewName)
//      AzureServiceBusSynapsePOC.logMessageSQL("EtlPipeline.onScheduleViewDrop", "Completed")
//    }
}
