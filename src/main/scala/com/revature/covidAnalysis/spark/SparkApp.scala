package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, col, count, expr, max, min, udf}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.ml.feature.StringIndexer
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{CharType, DataType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}

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

  /*
   * sparkAnalysis will run analytics on Dataframes/Datasets using Spark SQL
   */
  def sparkAnalysis(): Unit ={

    //load csv into dataframe from HDFS
    val covid19Data = sparkLoadCovidData(sparkRun())
    val confirmed = sparkLoadCSV(sparkRun(), "time_series_covid_19_confirmed.csv")
    val confirmedUS = sparkLoadCSV(sparkRun(), "time_series_covid_19_confirmed_US.csv")
    val deaths = sparkLoadCSV(sparkRun(), "time_series_covid_19_deaths.csv")
    val deathsUS = sparkLoadCSV(sparkRun(), "time_series_covid_19_deaths_US.csv")
    val recovered = sparkLoadCSV(sparkRun(), "time_series_covid_19_recovered.csv")

    /*Sample Question: Which Province/State has the most deaths?*/
    covid19Data.createOrReplaceTempView("covid19data")
    val countryMostDeath = sparkRun.sql(
      """
        | SELECT ObservationDate, `Province/State`, `Country/Region`, Deaths
        | FROM covid19data order by Deaths desc limit 1
    """.stripMargin
    )
    println("Which Province/State has the most deaths?")
    countryMostDeath.show()
  }

  /* sparkLoadCovidData will return a dataframe of
  * covid_19_data.csv
  * */
  def sparkLoadCovidData(spark: SparkSession): DataFrame ={
    //File 1: covid_19_data.csv

    //Schema structure
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
    val userFile = sparkRun.read
      .option("header", "true")
      .option("sep", ",")
      .option("dateFormat", "MM/dd/yyyy")
      .option("nullValue", "")
      //.option("inferSchema", "true")
      .schema(covid19dataSchema)
      .csv(hdfsLocation.hdfs_path + "covid_19_data.csv")
      .toDF()

    userFile
  }
  /*sparkLoadInferSchema loads in the spark session and csvFile name and returns a dataframe of that CSV with
  * an inferred schema
   */
  def sparkLoadCSV(spark: SparkSession, csvFile : String): DataFrame ={
    val userFile = sparkRun.read
      .option("header", "true")
      .option("sep", ",")
      .option("dateFormat", "MM/dd/yy")
      .option("nullValue", "")
      .option("inferSchema", "true")
      .csv(hdfsLocation.hdfs_path + csvFile)
      .toDF()

    userFile
  }

}
