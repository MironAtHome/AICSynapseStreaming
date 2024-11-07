package com.microsoft.azure.poc

import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write

object RowJsonSerializer {

  implicit val formats = Serialization.formats(org.json4s.NoTypeHints)

  def toJSON(
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
            ) = {
    write(UCDD(
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
      ,  resourceUnits_Scaled: String ))
  }
}