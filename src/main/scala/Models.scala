package com.microsoft.azure.poc

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, CalendarIntervalType, DateType, DoubleType, HiveStringType, LongType, MapType, NullType, NumericType, ObjectType, StringType, StructType, TimestampType}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

class Models {

  def ucddDFSchemaGet(): StructType = {

    val ucddDFSchema = new StructType()
      .add("AccountId", StringType, true)
      .add("AdditionalInfo", StringType, true)
      .add("BCToUSDExchangeRate", StringType, true)
      .add("BillableItemId", LongType, true)
      .add("BillingMonth", StringType, true)
      .add("BillingMonthdt", StringType, true)
      .add("ChannelType", StringType, true)
      .add("ConsumedService", StringType, true)
      .add("ConsumedServiceId", LongType, true)
      .add("DepartmentId", StringType, true)
      .add("EnrollmentId", StringType, true)
      .add("EnrollmentNumber_Hie", StringType, true)
      .add("EnrollmentNumber_UST", StringType, true)
      .add("InstanceName", StringType, true)
      .add("IsMonetaryCommitmentService", BooleanType, true)
      .add("MeterId", StringType, true)
      .add("MeterResourceId", StringType, true)
      .add("OfferId", StringType, true)
      .add("PartnerId", StringType, true)
      .add("ResourceGroup", StringType, true)
      .add("ResourceLocationId", LongType, true)
      .add("ResourceLocationNormalized", StringType, true)
      .add("ResourceType", StringType, true)
      .add("ServiceInfo1", StringType, true)
      .add("ServiceInfo2", StringType, true)
      .add("Sku", StringType, true)
      .add("SourceRegion", StringType, true)
      .add("StoreServiceIdentifier", StringType, true)
      .add("SubscriptionGuid", StringType, true)
      .add("SubscriptionId", StringType, true)
      .add("Tags", StringType, true)
      .add("TotalCharges", DoubleType, true)
      .add("USDTotalCharges", DoubleType, true)
      .add("UnitPrice", StringType, true)
      .add("UnitPriceUSD", StringType, true)
      .add("UnitPrice_Scaled", StringType, true)
      .add("UnitPrice_ScaledUSD", StringType, true)
      .add("UsageDate", StringType, true)
      .add("UsageDatedt", StringType, true)
      .add("maxUsageTime", StringType, true)
      .add("maxVortexIngestTime", StringType, true)
      .add("minUsageTime", StringType, true)
      .add("minVortexIngestTime", StringType, true)
      .add("resourceUnits", DoubleType, true)
      .add("resourceUnits_Scaled", DoubleType, true)

    ucddDFSchema

  }
}


case class UsageRequestAsyncPost (
                                   val data:String
                                   ,val RequestId:String
                                 )

case class UsageRequestCompletionAsyncPost (
                                   val data:String
                                   ,val RequestJSON:String
                                 )

case class CustomerUsageMessage(val RequestId:String
                                ,val RequestIdentityType:String
                                ,val Id:String
                                ,val ReportingIntervalStart:String
                                ,val ReportingTimeIntervalType:String
                                ,val ReportingIntervalDuration:String)


case class UCDDStringified(
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
               )

case class UCDD(
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
               )