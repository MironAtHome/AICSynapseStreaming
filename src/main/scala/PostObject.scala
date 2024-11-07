package com.microsoft.azure.poc

import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.nio.client.HttpAsyncClients
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import java.util.ArrayList
import org.apache.http.message.BasicNameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity

object PostObject extends Serializable{

  val usageSQLSerlializerUrl:String = "https://sb-synapse-poc-functionapp.azurewebsites.net/api/SB-Synapse-SQL-Serializer?code=2D1saSb1Fz1hDFedacW36rMauzaAENooMSQ9erhnadqhEXFf1pksZQ=="

  val requestConfig :RequestConfig = RequestConfig.custom()
    .setSocketTimeout(600000)
    .setConnectTimeout(600000).build()

  val httpAsyncClient :CloseableHttpAsyncClient = HttpAsyncClientBuilder.create().setDefaultRequestConfig(requestConfig).build()

  def startClient(): Unit = {
    httpAsyncClient.start()
  }

  def stopClient(): Unit = {
    httpAsyncClient.close()
  }

  def WriteBlobRow(jsonValue:String, mapId:Long) = {

    if(httpAsyncClient.isRunning == false){
      httpAsyncClient.start()
    }

    val postRequest: HttpPost = new HttpPost(usageSQLSerlializerUrl)
    val nameValuePairs = new ArrayList[BasicNameValuePair]()
    nameValuePairs.add(new BasicNameValuePair("jsonValue", jsonValue))
    nameValuePairs.add(new BasicNameValuePair("mapId", mapId.toString))
    postRequest.setEntity(new UrlEncodedFormEntity(nameValuePairs))
    postRequest.setHeader("Accept", "application/json")
    postRequest.setHeader("Content-type", "application/json")
    val response = httpAsyncClient.execute(postRequest, null)
  }
}