package com.wing.sql

import org.apache.spark.sql.SparkSession

object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate();

    val people = sparkSession.read.json(args(0))
    people.show()

    sparkSession.stop()

  }

}
