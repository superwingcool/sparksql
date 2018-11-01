package com.wing.sql

import org.apache.spark.sql.SparkSession

object DataSetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataSetApp")
      .master("local[2]")
      .getOrCreate()

    val df = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter",";")
      .csv("/tmp/people.csv")

    import sparkSession.implicits._
    val ds = df.as[People]

    ds.show()

    ds.map(line => line.name).show()

    sparkSession.stop()

  }

  case class People(name:String, age:Int, job:String)
}
