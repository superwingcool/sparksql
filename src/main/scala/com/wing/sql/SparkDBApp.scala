package com.wing.sql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkDBApp {

  def main(args: Array[String]): Unit = {

    val connectionProperties = new Properties()
    connectionProperties.put("user", "root")
    connectionProperties.put("password", "123456")
    connectionProperties.put("driver", "com.mysql.jdbc.Driver")


    val spark = SparkSession
      .builder()
      .appName("SparkDBApp")
      .master("local[2]")
      .getOrCreate()

    val jdbcDF = spark.read
      .jdbc("jdbc:mysql://localhost:3306", "sell.product_info", connectionProperties)

    jdbcDF.printSchema()

    val data = jdbcDF.select("product_id", "product_name", "product_price").filter(jdbcDF.col("product_price") > 2)

    data.write
      .option("createTableColumnTypes", "product_id int, product_name VARCHAR(50), product_price VARCHAR(20)")
      .jdbc("jdbc:mysql://localhost:3306", "sell.new_product", connectionProperties)

    spark.stop()

  }

}
