package com.microsoft.azure.poc


import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import java.nio.charset.StandardCharsets.UTF_8

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
import org.apache.http.client.entity.EntityBuilder
import org.apache.http.entity.ContentType
import org.apache.http.entity.BasicHttpEntity
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
import java.net.URLEncoder

import scala.collection.mutable.MutableList
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
import java.time.Instant
import java.time.LocalDate
/*
import java.net.URI

import com.google.gson.GsonBuilder
import com.microsoft.azure.poc.{AzureServiceBusSynapsePOC, CustomerUsageMessage, RowJsonSerializer, UsageRequestCompletionAsyncPost}
import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.http.{HttpEntity, HttpResponse}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.concurrent.FutureCallback
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClientBuilder}
import org.apache.http.util.EntityUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct, to_json}
import org.json4s.jackson.JsonMethods.{compact, render}

import scala.util.{Failure, Success}
import scala.util.control.Breaks.{break, breakable}

*/

object MessageStreamingPipeline {

  val viewName = "UCDD"
  val viewEnrSubResolverName = "UCDDEnrSubRes"
  val viewAcctSubResolverName = "UCDDAcctSubRes"

  private[this] var currentViewPartition: String = "01"

  val viewPartitionIndex:Array[String] = Array[String] ("01", "02")

  def getNextPartitionIndex(currentIndex:String): String ={
    var result:String = ""
    var x:Int = 0
    while(x < viewPartitionIndex.length){
      if(viewPartitionIndex(x).equals(currentIndex)){
        if(x == (viewPartitionIndex.length-1)){
          result = viewPartitionIndex(0)
        } else {
          x = (x+1)
          result = viewPartitionIndex(x)
        }
        x = viewPartitionIndex.length
      }
      x += 1
    }
    result
  }

  def setNextPartitionIndex(): Unit = synchronized {
    currentViewPartition = getNextPartitionIndex(currentViewPartition)
  }

  def getCurrentPartitionIndex(): String = synchronized {
    currentViewPartition
  }
}

class MessageStreamingPipeline(parentSpark :SparkSession) extends Thread {

  // give, respectively, 5 seconds timeout for connect and for intra packet rate distance
  val requestConfig :RequestConfig = RequestConfig.custom()
    .setSocketTimeout(600000)
    .setConnectTimeout(600000).build();

  val httpClient :CloseableHttpClient = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build()

  val usageMessageReceiverUrl:String = "https://sb-synapse-poc-functionapp.azurewebsites.net/api/SB-Synapse-Queue-Message-Retriever?code=2D1saSb1Fz1hDFedacW36rMauzaAENooMSQ9erhnadqhEXFf1pksZQ=="
  val mapIdForRequestIdUrl:String = "https://sb-synapse-poc-functionapp.azurewebsites.net/api/SB-Synapse-RequestId-Mapper?code=2D1saSb1Fz1hDFedacW36rMauzaAENooMSQ9erhnadqhEXFf1pksZQ=="
  val requestCompletionUrl:String = "https://sb-synapse-poc-functionapp.azurewebsites.net/api/SB-Synapse-Request-Completion?code=2D1saSb1Fz1hDFedacW36rMauzaAENooMSQ9erhnadqhEXFf1pksZQ=="

  final val HTTP_PAYLOAD_BUFFER = 2048 //this is how many bytes will be used to consume data from the wire

  val pipelineSpark :SparkSession = parentSpark.newSession()
  import pipelineSpark.implicits._

  val GSON = new GsonBuilder().create()

  override def run() {
    try {
      PostObject.startClient()
      AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", "entered run method")

      breakable {
        while (true) {
          var earlyCheck = false;
          try{
            earlyCheck = pipelineSpark.catalog.tableExists( s"global_temp.${MessageStreamingPipeline.viewName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())}")
          } catch {
            case ex: Throwable => {
              val message = s"Exception of type ${ex.getClass().getSimpleName()}. Error message: ${ex.getMessage()}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
              AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", message)
            }
          }
          if (earlyCheck) {
            break
          }
          AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", s"Check for global_temp.${MessageStreamingPipeline.viewName}${MessageStreamingPipeline.getCurrentPartitionIndex()} indicates $earlyCheck before starting work cycle")
          Thread.sleep(10000)
        }
      }

      var x: Int = 0
      val messageCount = 100
      var messageIndex = 0
      while (1 == 1) {
        x += 1
        messageIndex = 0
        AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", s"About to request batch of $messageCount message(s) from Azure Service Bus")
        var userRequests = getQueueMessages(messageCount)
        var requestStatus = ListBuffer[String]()
        userRequests.foreach((req) => {
          messageIndex += 1
          val id = req.Id
          val requestId = req.RequestId
          val reqJSON = GSON.toJson(req,classOf[CustomerUsageMessage])
          var fileWriteIndex = 0
          //val fileWriteMaxAttempts = 5
          //while (fileWriteIndex < fileWriteMaxAttempts) {
          //  fileWriteIndex += 1
          AzureServiceBusSynapsePOC.logMessageSQL(s"MessageStreamingPipeline.run.${req.RequestId}", s"Query against id $id, batch $x started")
          try {

            val mapId:Long = mapIdForRequestId(id,reqJSON)
            val mapIdBroadcast = pipelineSpark.sparkContext.broadcast[Long](mapId)
            
            var result :DataFrame =  null.asInstanceOf[DataFrame]
            if (req.RequestIdentityType == "EnrollmentId") {
                result = pipelineSpark.sql(s"SELECT ucdd.EnrollmentId, ucdd.AccountId, ucdd.AdditionalInfo, ucdd.BCToUSDExchangeRate, ucdd.BillableItemId, ucdd.BillingMonth, ucdd.BillingMonthdt, ucdd.ChannelType, ucdd.ConsumedService, ucdd.ConsumedServiceId, ucdd.DepartmentId, ucdd.EnrollmentNumber_Hie, ucdd.EnrollmentNumber_UST, ucdd.InstanceName, ucdd.IsMonetaryCommitmentService, ucdd.MeterId, ucdd.MeterResourceId, ucdd.OfferId, ucdd.PartnerId, ucdd.ResourceGroup, ucdd.ResourceLocationId, ucdd.ResourceLocationNormalized, ucdd.ResourceType, ucdd.ServiceInfo1, ucdd.ServiceInfo2, ucdd.Sku, ucdd.SourceRegion, ucdd.StoreServiceIdentifier, ucdd.SubscriptionGuid, ucdd.SubscriptionId, ucdd.Tags, ucdd.TotalCharges, ucdd.USDTotalCharges, ucdd.UnitPrice, ucdd.UnitPriceUSD, ucdd.UnitPrice_Scaled, ucdd.UnitPrice_ScaledUSD, ucdd.UsageDate, ucdd.UsageDatedt, ucdd.maxUsageTime, ucdd.maxVortexIngestTime, ucdd.minUsageTime, ucdd.minVortexIngestTime, ucdd.resourceUnits, ucdd.resourceUnits_Scaled FROM global_temp.${MessageStreamingPipeline.viewName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} ucdd INNER JOIN global_temp.${MessageStreamingPipeline.viewEnrSubResolverName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} ucddsubres ON ucddsubres.SubscriptionId = ucdd.SubscriptionId WHERE ucddsubres.EnrollmentId = $id")
            }
            if (req.RequestIdentityType == "SubscriptionId") {
                result = pipelineSpark.sql(s"SELECT ucdd.EnrollmentId, ucdd.AccountId, ucdd.AdditionalInfo, ucdd.BCToUSDExchangeRate, ucdd.BillableItemId, ucdd.BillingMonth, ucdd.BillingMonthdt, ucdd.ChannelType, ucdd.ConsumedService, ucdd.ConsumedServiceId, ucdd.DepartmentId, ucdd.EnrollmentNumber_Hie, ucdd.EnrollmentNumber_UST, ucdd.InstanceName, ucdd.IsMonetaryCommitmentService, ucdd.MeterId, ucdd.MeterResourceId, ucdd.OfferId, ucdd.PartnerId, ucdd.ResourceGroup, ucdd.ResourceLocationId, ucdd.ResourceLocationNormalized, ucdd.ResourceType, ucdd.ServiceInfo1, ucdd.ServiceInfo2, ucdd.Sku, ucdd.SourceRegion, ucdd.StoreServiceIdentifier, ucdd.SubscriptionGuid, ucdd.SubscriptionId, ucdd.Tags, ucdd.TotalCharges, ucdd.USDTotalCharges, ucdd.UnitPrice, ucdd.UnitPriceUSD, ucdd.UnitPrice_Scaled, ucdd.UnitPrice_ScaledUSD, ucdd.UsageDate, ucdd.UsageDatedt, ucdd.maxUsageTime, ucdd.maxVortexIngestTime, ucdd.minUsageTime, ucdd.minVortexIngestTime, ucdd.resourceUnits, ucdd.resourceUnits_Scaled FROM global_temp.${MessageStreamingPipeline.viewName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} WHERE SubscriptionId = $id")
            }
            if (req.RequestIdentityType == "AccountId") {
                result = pipelineSpark.sql(s"SELECT ucdd.EnrollmentId, ucdd.AccountId, ucdd.AdditionalInfo, ucdd.BCToUSDExchangeRate, ucdd.BillableItemId, ucdd.BillingMonth, ucdd.BillingMonthdt, ucdd.ChannelType, ucdd.ConsumedService, ucdd.ConsumedServiceId, ucdd.DepartmentId, ucdd.EnrollmentNumber_Hie, ucdd.EnrollmentNumber_UST, ucdd.InstanceName, ucdd.IsMonetaryCommitmentService, ucdd.MeterId, ucdd.MeterResourceId, ucdd.OfferId, ucdd.PartnerId, ucdd.ResourceGroup, ucdd.ResourceLocationId, ucdd.ResourceLocationNormalized, ucdd.ResourceType, ucdd.ServiceInfo1, ucdd.ServiceInfo2, ucdd.Sku, ucdd.SourceRegion, ucdd.StoreServiceIdentifier, ucdd.SubscriptionGuid, ucdd.SubscriptionId, ucdd.Tags, ucdd.TotalCharges, ucdd.USDTotalCharges, ucdd.UnitPrice, ucdd.UnitPriceUSD, ucdd.UnitPrice_Scaled, ucdd.UnitPrice_ScaledUSD, ucdd.UsageDate, ucdd.UsageDatedt, ucdd.maxUsageTime, ucdd.maxVortexIngestTime, ucdd.minUsageTime, ucdd.minVortexIngestTime, ucdd.resourceUnits, ucdd.resourceUnits_Scaled FROM global_temp.${MessageStreamingPipeline.viewName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} ucdd INNER JOIN global_temp.${MessageStreamingPipeline.viewAcctSubResolverName.concat(MessageStreamingPipeline.getCurrentPartitionIndex())} ucddsubres ON ucddsubres.SubscriptionId = ucdd.SubscriptionId WHERE ucddsubres.AccountId = $id")
            }
            
            result.select(to_json(struct(col("*"))).alias("content")).rdd.foreachPartitionAsync(iter => {
                var requestProgress = new ListBuffer[(Instant,Int)]()
                var rowBuffer = new StringBuilder()
                var messageCounter = 1
                var rowsCollected = false
                iter.foreach(row => {
                  if(messageCounter == 1){
                    rowBuffer.clear()
                    rowBuffer.append("[")
                    rowBuffer.append(row.getString(row.fieldIndex("content")))
                    rowBuffer.append(",")
                  }
                  if(messageCounter < 5000){
                    rowBuffer.append(row.getString(row.fieldIndex("content")))
                    rowBuffer.append(",")
                  } else {
                    rowBuffer.append(row.getString(row.fieldIndex("content")))
                    rowBuffer.append("]")
                    PostObject.WriteBlobRow(rowBuffer.result(), mapIdBroadcast.value)
                    requestProgress.+=((systemUTC.instant(),messageCounter))
                    messageCounter = 0
                    rowBuffer.clear()
                  }
                  messageCounter += 1
                })
                if(messageCounter > 1) {
                  rowBuffer.append("]")
                  PostObject.WriteBlobRow(rowBuffer.result(), mapIdBroadcast.value)
                  rowBuffer.clear()
                  requestProgress.+=((systemUTC.instant(),messageCounter))
                }
              }).onComplete {
              case Success(v) => {
                  sendRequestCompletionMessage(mapId, "SUCCESS")
                  requestStatus.+=(s"$mapId:SUCCESS")
                }
              case Failure (t) => {
                sendRequestCompletionMessage(mapId, "FAILURE")
                requestStatus.+=(s"$mapId:FAILURE:${t.getMessage}:${ExceptionUtils.getStackTrace(t)}")
              }
            }
          } catch {
            case ex: Throwable => {
              val errorMessage = s"Exception of type ${ex.getClass().getSimpleName()}. Error message: ${ex.getMessage()}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
              AzureServiceBusSynapsePOC.logMessageSQL(s"MessageStreamingPipeline.run.${req.RequestId}", errorMessage)
              Thread.sleep(0)
            }
          }
          //}
        })
        requestStatus.foreach(r => {
          AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run.requestStatus", r)
        })
        Thread.sleep(0) // yield to other threads cycles

        AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", s"Batch #$x of $messageIndex message(s) from Azure Service Bus completed successfully")
      }
    } catch {
      case ex: Throwable => {
        val message = s"Exception of type ${ex.getClass().getSimpleName()}. Error message: ${ex.getMessage()}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
        AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", message)
      }
    } finally {
      AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.run", "Exiting")
      httpClient.close()
      PostObject.stopClient()
    }
  }

  /**
   * Sends RESTfull request to retrieves messages from Service Bus Queue via Azure Function
   * @param mapIdRequestId Long value mapped to original GUID of user requet id
   * @statusMessage string to log
   */
  def sendRequestCompletionMessage(mapIdRequestId:Long, statusMessage:String): Unit = {

    var mapId :Long = 0

    val uri: URI = new URIBuilder(requestCompletionUrl)
      .addParameter("mapIdRequestId",mapIdRequestId.toString)
      .addParameter("statusMessage",URLEncoder.encode(statusMessage,UTF_8.toString()))
      .build()
    val getRequest = new HttpGet(uri)
    val httpResponse = httpClient.execute(getRequest)
    var jsonString = parseHttpResponse(httpResponse)
  }


  /**
   * Sends RESTfull request to retrieves messages from Service Bus Queue via Azure Function
   * @param RequestId GUID unique identifier of the user request
   * @param requestJson complete text of user request
   * In plain English language we say "please give me from 0 and up to messageCount messages in 1 second or less time"
   */
  def mapIdForRequestId(RequestId:String, requestJson:String): Long = {

    var mapId :Long = 0

    val uri: URI = new URIBuilder(mapIdForRequestIdUrl)
      .addParameter("RequestId",RequestId)
      .addParameter("requestJson",URLEncoder.encode(requestJson,UTF_8.toString()))
      .build()
    val getRequest = new HttpGet(uri)
    val httpResponse = httpClient.execute(getRequest)
    var jsonString = parseHttpResponse(httpResponse)
    if (jsonString != null) {
      mapId = GSON.fromJson(jsonString, classOf[Long])
    }
    mapId
  }

  /**
   * Sends RESTfull request to retrieves messages from Service Bus Queue via Azure Function
   * @param messageCount Int msximum number of messages application would like to RSVP in 1 second
   * In plain English language we say "please give me from 0 and up to messageCount messages in 1 second or less time"
   */
  def getQueueMessages(messageCount:Int): Array[CustomerUsageMessage] = {

    var userRequests :Array[CustomerUsageMessage] = null.asInstanceOf[Array[CustomerUsageMessage]]

    val uri: URI = new URIBuilder(usageMessageReceiverUrl)
      .addParameter("messageCount",messageCount.toString())
      .build()
    val getRequest = new HttpGet(uri)
    val httpResponse = httpClient.execute(getRequest)
    var jsonString = parseHttpResponse(httpResponse)
    if (jsonString != null) {
      userRequests = GSON.fromJson(jsonString, classOf[Array[CustomerUsageMessage]])
    }
    userRequests
  }

  /**
   * @method parseHttpResponse
   * Serializes HttpResponse header to byte array.
   * @param response http response object
   * @return json string
   */
  def parseHttpResponse(response: HttpResponse): String = {
    try {
      if (response == null) {
        null
      } else {
        val he :HttpEntity = response.getEntity()
        var encoding = ""
        if(he.getContentEncoding() == null)
        {
          encoding = "UTF-8"
        } else {
          encoding = he.getContentEncoding().getValue()
        }
        val jsonString = EntityUtils.toString(he, encoding)
        AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.parseHttpResponse", s"Received: $jsonString")
        jsonString
      }
    } catch {
      case ex:Throwable => {
        val message = s"Exception of type ${ex.getClass().getSimpleName()}. Error message: ${ExceptionUtils.getMessage(ex)}. Stack trace: ${ExceptionUtils.getStackTrace(ex)}"
        AzureServiceBusSynapsePOC.logMessageSQL("MessageStreamingPipeline.parseHttpResponse",message)
        null.asInstanceOf[String]
      }
    }
  }

  // DataFrame to JSON string serialization
  // https://stackoverflow.com/questions/36157810/spark-row-to-json
  def UCDDToJSON(
                  AccountId: String
                  ,  AdditionalInfo: String
                  ,  BCToUSDExchangeRate: String
                  ,  BillableItemId: String
                  ,  BillingMonth: String
                  ,  BillingMonthdt: String
                  ,  ChannelType: String
                  ,  ConsumedService: String
                  ,  ConsumedServiceId: String
                  ,  DepartmentId: String
                  ,  EnrollmentId: String
                  ,  EnrollmentNumber_Hie: String
                  ,  EnrollmentNumber_UST: String
                  ,  InstanceName: String
                  ,  IsMonetaryCommitmentService: String
                  ,  MeterId: String
                  ,  MeterResourceId: String
                  ,  OfferId: String
                  ,  PartnerId: String
                  ,  ResourceGroup: String
                  ,  ResourceLocationId: String
                  ,  ResourceLocationNormalized: String
                  ,  ResourceType: String
                  ,  ServiceInfo1: String
                  ,  ServiceInfo2: String
                  ,  Sku: String
                  ,  SourceRegion: String
                  ,  StoreServiceIdentifier: String
                  ,  SubscriptionGuid: String
                  ,  SubscriptionId: String
                  ,  Tags: String
                  ,  TotalCharges: String
                  ,  USDTotalCharges: String
                  ,  UnitPrice: String
                  ,  UnitPriceUSD: String
                  ,  UnitPrice_Scaled: String
                  ,  UnitPrice_ScaledUSD: String
                  ,  UsageDate: String
                  ,  UsageDatedt: String
                  ,  maxUsageTime: String
                  ,  maxVortexIngestTime: String
                  ,  minUsageTime: String
                  ,  minVortexIngestTime: String
                  ,  resourceUnits: String
                  ,  resourceUnits_Scaled: String
                ): String = {
    RowJsonSerializer.toJSON(AccountId, AdditionalInfo, BCToUSDExchangeRate, BillableItemId, BillingMonth, BillingMonthdt, ChannelType, ConsumedService, ConsumedServiceId, DepartmentId, EnrollmentId, EnrollmentNumber_Hie, EnrollmentNumber_UST, InstanceName, IsMonetaryCommitmentService, MeterId, MeterResourceId, OfferId, PartnerId, ResourceGroup, ResourceLocationId, ResourceLocationNormalized, ResourceType, ServiceInfo1, ServiceInfo2, Sku, SourceRegion, StoreServiceIdentifier, SubscriptionGuid, SubscriptionId, Tags, TotalCharges, USDTotalCharges, UnitPrice, UnitPriceUSD, UnitPrice_Scaled, UnitPrice_ScaledUSD, UsageDate, UsageDatedt, maxUsageTime, maxVortexIngestTime, minUsageTime, minVortexIngestTime, resourceUnits, resourceUnits_Scaled)
  }
}