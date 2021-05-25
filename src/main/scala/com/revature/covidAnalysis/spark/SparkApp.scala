package com.revature.covidAnalysis.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{min, max, avg, expr, udf, count}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.StringIndexer
import org.apache.log4j.Logger
import org.apache.log4j.Level


class SparkApp {

  def sparkRun() : SparkSession = {
    val spark = {
      SparkSession
        .builder
        .appName("Covid-19 Analysis")
        //.master("local")
        .config("spark.master", "local")
        .config("spark.eventLog.enabled", false)
        .getOrCreate()
    }
    spark
  }

  def sparkClose(): Unit ={
    sparkRun.close()
  }

  //import spark.implicits._
  def sparkTest(): Unit ={
    println("Spark Test")
  }
}
