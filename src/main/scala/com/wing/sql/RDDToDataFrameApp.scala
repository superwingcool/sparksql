package com.wing.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object RDDToDataFrameApp {

  def main(args: Array[String]): Unit = {

    val sparkconf = new SparkConf().setMaster("local[2]").setAppName("RDDToDataFrameApp")
    val spark = SparkSession.builder().config(sparkconf).getOrCreate()

    val rdd = spark.sparkContext.textFile("file:///tmp/people.txt")

    refection(spark, rdd)

    //program(spark, rdd)

    spark.stop()
  }

  //编程式（RDD to DataFrame）
  //不需要确定每个属性的类型
  private def program(spark: SparkSession, rdd: RDD[String]) = {
    val schemas = Array("id", "name", "age")
    val fields = schemas.map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val rowRDD = rdd
      .map(_.split(","))
      .map(attributes => Row(attributes(0), attributes(1).trim, attributes(2)))

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.printSchema()
    peopleDF.show()

    peopleDF.createOrReplaceTempView("people_view")

    spark.sql("select * from people_view where age < 30").show()
  }


  //反射机制（RDD to DataFrame）
  //必须确定每个属性的类型
  private def refection(spark: SparkSession, rdd: RDD[String]) = {
    import spark.implicits._
    val peopleDF = rdd.map(_.split(","))
      .map(p => new Person(p(0).toInt, p(1).toString, p(2).toInt)).toDF()

    val peopleDF1 = rdd.map(_.split(","))
      .map(p => new Person(p(0).toInt, p(1).toString, p(2).toInt)).toDF()

    peopleDF.printSchema()
    peopleDF.show()

    peopleDF.filter(peopleDF.col("age") >= 30).show()

    peopleDF.createOrReplaceTempView("people_view")

    spark.sql("select * from people_view where age < 30").show()

    peopleDF.join(peopleDF1, peopleDF.col("id") === peopleDF1.col("id")).show()
  }
}

case class Person(id: Int, name: String, age: Int)
