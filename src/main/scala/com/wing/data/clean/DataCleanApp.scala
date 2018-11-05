package com.wing.data.clean

import com.wing.data.util.DateUtils
import org.apache.spark.sql.SparkSession

object DataCleanApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("DataCleanApp")
      .master("local[2]")
      .getOrCreate()

    val accessLog = spark.sparkContext.textFile("/Users/anyi/Downloads/data/access.20161111.log")

    //accessLog.take(10).foreach(println)

    //accessLog.show()

    import spark.implicits._

    accessLog.map(line => {
      val datas = line.split(" ")
      val ip = datas(0)
      val time = datas(3) + " " + datas(4)

      val url = datas(11).replaceAll("\"", "")
      val traffic = datas(9)
      //(DateUtils.parse(time), url, traffic, ip)
      DateUtils.parse(time) + "\t" + url + "\t" + traffic + "\t" + ip
    }).saveAsTextFile("/Users/anyi/Downloads/data/output/")


    spark.stop()


  }

}
