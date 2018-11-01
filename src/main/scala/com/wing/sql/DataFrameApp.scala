package com.wing.sql

import org.apache.spark.sql.SparkSession

object DataFrameApp {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("DataFrameApp")
      .master("local[2]")
      .getOrCreate()

    // DataFrame is a Dataset organized into named columns.
    // It is conceptually equivalent to a table in a relational database or a data frame in R/Python, but with richer optimizations under the hood.
    // DataFrames can be constructed from a wide array of sources such as: structured data files, tables in Hive, external databases, or existing RDDs.
    val peopleDataFrame = sparkSession.read.json("file:///tmp/people.json")

    //打印Dataframe schema
    peopleDataFrame.printSchema()

    peopleDataFrame.show()

    //select
    peopleDataFrame.select("name").show()

    //select
    peopleDataFrame.select(peopleDataFrame.col("name"), (peopleDataFrame.col("age") + 10).as("age2")).show()

    //过滤
    peopleDataFrame.filter(peopleDataFrame.col("age") > 19).show()

    //分组，聚合
    peopleDataFrame.groupBy("age").count().show()

    sparkSession.stop()

  }

}
