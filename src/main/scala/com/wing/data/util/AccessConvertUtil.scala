package com.wing.data.util


import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object AccessConvertUtil {

  val schemas = StructType(Array(
    StructField("url", StringType),
    StructField("cmsType", StringType),
    StructField("cmsId", LongType),
    StructField("traffic", LongType),
    StructField("ip", StringType),
    StructField("city", StringType),
    StructField("time", StringType),
    StructField("day", StringType)
  ))

  private val DOMAIN = "http://www.imooc.com/"

  def parse(log: String): Row = {

    try {

      val logDatas = log.split("\t")
      val url = logDatas(1)
      val traffic = logDatas(2).toLong
      val ip = logDatas(3)

      val cmsInfo = url.substring(DOMAIN.length).split("/")
      var cmsType = ""
      var cmsId = 0l
      if (cmsInfo.length > 1) {
        cmsType = cmsInfo(0)
        cmsId = cmsInfo(1).toLong
      }

      val city = ""

      val time = logDatas(0)
      val day = time.substring(0, 10).replaceAll("-", "")

      Row(url, cmsType, cmsId, traffic, ip, city, time, day)
    } catch {
      case e: Exception => Row(0)
    }
  }


}
