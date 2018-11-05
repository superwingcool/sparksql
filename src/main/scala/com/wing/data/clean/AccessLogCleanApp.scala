package com.wing.data.clean

import com.wing.data.util.AccessConvertUtil
import org.apache.spark.sql.SparkSession

object AccessLogCleanApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .appName("AccessLogCleanApp")
      .master("local[2]")
      .getOrCreate()

    val accessLogRDD = sparkSession.sparkContext.textFile("/Users/anyi/Downloads/data/access.log")


    val accessDF = sparkSession.createDataFrame(accessLogRDD.map(x => AccessConvertUtil.parse(x)),
      AccessConvertUtil.schemas)

    accessDF.show()

  }
}
