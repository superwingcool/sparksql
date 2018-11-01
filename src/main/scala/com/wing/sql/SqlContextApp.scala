package com.wing.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object SqlContextApp {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    //sparkConf.setAppName("sql_context_test").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val path = args(0);

    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()

    sc.stop();

  }
}
