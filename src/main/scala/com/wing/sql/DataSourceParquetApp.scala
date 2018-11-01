package com.wing.sql

import org.apache.spark.sql.SparkSession

object DataSourceParquetApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()
    val df = sparkSession.read.parquet("/tmp/users.parquet")
    //df.show()
//    import sparkSession.implicits._
//    df.as[User].select()
    df.select("name", "favorite_color").write.json("/tmp/users.json")
    sparkSession.stop()
  }

  case class User(name:String, favorite_color:String, favorite_numbers:Array[String])
}
