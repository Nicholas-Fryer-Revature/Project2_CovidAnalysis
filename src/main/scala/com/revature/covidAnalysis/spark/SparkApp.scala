package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, expr, max, min, udf}
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
/*  covid_19_data.csv"
    time_series_covid_19_confirmed.csv"
    projectdata/time_series_covid_19_confirmed_US.csv"
    time_series_covid_19_deaths.csv"
    time_series_covid_19_deaths_US.csv"
    time_series_covid_19_recovered.csv"
    */

    //Test Dataframe to load HDFS
    val userFile = sparkRun.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",")
      .option("nullValue", "")
      .csv(hdfsLocation.hdfs_path + "covid_19_data.csv")
      .toDF()

    userFile.show()
  }

}
