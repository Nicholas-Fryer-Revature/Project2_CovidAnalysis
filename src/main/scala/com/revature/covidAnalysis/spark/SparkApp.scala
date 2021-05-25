package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, count, expr, max, min, udf}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.StringIndexer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{CharType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}



class SparkApp {
  Logger.getLogger("org").setLevel(Level.ERROR)
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
/*  FILE NAMES
    covid_19_data.csv"
    time_series_covid_19_confirmed.csv"
    projectdata/time_series_covid_19_confirmed_US.csv"
    time_series_covid_19_deaths.csv"
    time_series_covid_19_deaths_US.csv"
    time_series_covid_19_recovered.csv"
    */

    //Infer schema is giving issues with DateType so I'm going to make my own schema

    val covid19dataSchema = StructType {
      Array(
        StructField("SNo", IntegerType) ,
        StructField("ObservationDate", DateType) ,
        StructField("Province/State", StringType) ,
        StructField("Country/Region", StringType) ,
        StructField("Last Update", StringType) ,
        StructField("Confirmed", DoubleType) ,
        StructField("Deaths", DoubleType) ,
        StructField("Recovered", DoubleType) ,
      )
    }

    //Load covid_19_data.csv into a dataframe
    val userFile = sparkRun.read
      .option("header", "true")
      .option("sep", ",")
      .option("dateFormat", "MM/dd/yyyy")
      .option("nullValue", "")
      //.option("inferSchema", "true")
      .schema(covid19dataSchema)
      .csv(hdfsLocation.hdfs_path + "covid_19_data.csv")
      .toDF()
    println("Showing covid_19_data.csv")
    userFile.show()
    println("Printing Schema of covid_19_data.csv")
    userFile.printSchema()

    /*Question 1: Which Province/State has the most deaths?*/
    userFile.createOrReplaceTempView("covid19data")
    val countryMostDeath = sparkRun.sql(
    """
      | SELECT ObservationDate, `Province/State`, `Country/Region`, Deaths
      | FROM covid19data order by Deaths desc limit 1
    """.stripMargin
    )
    println("Which Province/State has the most deaths?")
    countryMostDeath.show()
  }

}
