package com.microsoft.azure.poc

import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.nio.charset.StandardCharsets

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, CalendarIntervalType, DateType, DoubleType, HiveStringType, LongType, MapType, NullType, NumericType, ObjectType, StringType, StructType, TimestampType}
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY
import org.apache.spark.SparkException
import org.apache.commons.lang3.exception.ExceptionUtils
import java.net.URI

import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.CallableStatement
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement
import com.microsoft.sqlserver.jdbc.SQLServerConnection

import java.io._

import org.apache.http.HttpEntity
import org.apache.http.entity.StringEntity
import org.apache.http.HttpResponse
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.ClientProtocolException
import org.apache.http.util.EntityUtils
import org.apache.http.entity.ContentType
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.impl.io.DefaultHttpResponseWriter
import org.apache.http.impl.io.HttpTransportMetricsImpl
import org.apache.http.impl.io.SessionOutputBufferImpl
import org.apache.http.io.HttpMessageWriter
import org.apache.http.nio.IOControl
//import java.util.concurrent.CountDownLatch

import org.apache.http.concurrent.FutureCallback
import org.apache.http.nio.client.methods.AsyncCharConsumer
import org.apache.http.nio.client.methods.HttpAsyncMethods
import org.apache.http.protocol.HttpContext
import java.io.IOException

import org.apache.commons.lang3.exception.ExceptionUtils

import scala.collection.mutable.StringBuilder
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.Writer
import java.nio.CharBuffer
import java.util.Scanner

import util.control.Breaks._
import com.microsoft.hyperspace._
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig
import org.json4s.jackson.JsonMethods.{compact, render}
import com.google.gson.GsonBuilder

import java.time.Clock.systemUTC

object AzureServiceBusSynapsePOC {

  val logConnectionUrl = "jdbc:sqlserver://ppepocsqlb01.database.windows.net;database=Instrumentation;user=SQLAdmin;password=****;loginTimeout=60;MultipleActiveResultSets=True"
  var logDBConnection = DriverManager.getConnection(logConnectionUrl)
  var logDBConnectionExceptionCounter: Int = 0
  val logDBConnectionExceptionMax = 10

  def main(args: Array[String]) {
    //Check whether sufficient params are supplied
    val spark = SparkSession.builder()
                .getOrCreate()
    import spark.implicits._

    spark.sparkContext.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe00.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe01.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe02.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe03.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe04.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe18.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe19.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe20.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe21.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe22.blob.core.windows.net", "****")
    spark.sparkContext.hadoopConfiguration.set("fs.azure.account.key.aciinteastusppe23.blob.core.windows.net", "****")


    spark.sql("set spark.sql.datetime.java8API.enabled=true")
    spark.sql("set spark.sql.files.ignoreCorruptFiles=true")
    spark.sql("set Spark.SQL.files.maxPartitionBytes=4294967296")
    spark.sql("set spark.sql.shuffle.partitions=158")
    spark.sql("set spark.sql.inMemoryColumnarStorage.compressed=true")
    spark.sql("set spark.sql.inMemoryColumnarStorage.batchSize=50000")
    // Hyperspace configuration settings
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)



    spark.enableHyperspace()

    try{

      val etlThread = new Thread(new EtlPipeline(spark))
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "etlThread instantiated")
      val streamThread = new Thread(new MessageStreamingPipeline(spark))
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "streamThread instantiated")

      etlThread.start()
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "etlThread started")
      streamThread.start()
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "streamThread started")
      streamThread.join()
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "streamThread joined")
      etlThread.join()
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "etlThread joined")

    } catch {
      case ex: Throwable => {
        val message = s"Exception of type ${ex.getClass().getSimpleName()}. Error message: ${ex.getMessage()}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
        AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", message)
        throw new SparkException("Exception in AzureServiceBusSynapsePOC.main, while waiting for response", ex)
      }
    } finally {
      AzureServiceBusSynapsePOC.logMessageSQL("AzureServiceBusSynapsePOC.main", "streamThread exited")
      if(logDBConnection != null && (!logDBConnection.isClosed())) {
        logDBConnection.close()
      }
      spark.close()
    }
  }

  def logMessageSQL(source: String, message: String): Unit = {

    try {

      val sourcePar = new StringBuilder()
      sourcePar.append(source)

      val messagePar = new StringBuilder()
      messagePar.append(message)

      val callableStatement: CallableStatement = logDBConnection.prepareCall("{call createLogRecord (?, ?)}")
      callableStatement.setNString(1, sourcePar.toString())
      callableStatement.setNString(2, messagePar.toString())
      callableStatement.execute()
      logDBConnectionExceptionCounter = 0;
    }
    catch {
      case ex: Throwable => {
        System.err.println(s"Error {call createLogRecord}. Message: ${ex.toString()}, stack trace: ${ExceptionUtils.getStackTrace(ex)}")
        logDBConnectionExceptionCounter += 1
        if (logDBConnectionExceptionCounter >= logDBConnectionExceptionMax) {
          throw new SparkException("Exception in AzureServiceBusSynapsePOC.logMessageSQL, while logging", ex)
        }
      }
    } finally {
      if (logDBConnection != null && logDBConnection.isValid(1) == false) {
        logDBConnection.close()
        logDBConnection = DriverManager.getConnection(logConnectionUrl)
      }
    }
  }
}

