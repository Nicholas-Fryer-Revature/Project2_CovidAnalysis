package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{sum,col}

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

    /*Sample Question: Which Province/State has the most/least deaths?*/
    covid19Data.createOrReplaceTempView("covid19data")
    val countryMostDeath = sparkRun.sql(
      """
        | SELECT ObservationDate, `Province/State`, `Country/Region`, Deaths
        | FROM covid19data order by Deaths desc limit 10
    """.stripMargin
    )
    println("Which Province/State has the most deaths?")
    countryMostDeath.show()

    val countryLeastDeath = sparkRun.sql(
      """
        | SELECT ObservationDate, `Province/State`, `Country/Region`, Deaths
        | FROM covid19data where Deaths > 0 order by Deaths limit 10
    """.stripMargin
    )
    println("Which Province/State has the least deaths?")
    countryLeastDeath.show()

    /*left off*/
    /*Sample Question: How fast is covid recovery compared to how often covid contraction?*/
    confirmed.createOrReplaceTempView("covid19confirmed")
    recovered.createOrReplaceTempView("covid19recovered")

    val worldRecoveredVContracted = sparkRun.sql(
      """
        | SELECT c.`Province/State`, c.`Country/Region`
        | FROM covid19confirmed c LEFT JOIN covid19recovered r ON (c.`Province/State` = r.`Province/State`)
        | LIMIT 10
    """.stripMargin
    )
    println("?")
    worldRecoveredVContracted.show()

    /*untouched question*/
    /*Sample Question: ow did the US recover compared to contraction
    (time_series_US confirmed vs time_series_covid recovered)*/
    confirmed.createOrReplaceTempView("covid19confirmed")
    recovered.createOrReplaceTempView("covid19recovered")

    val USRecoveredVContracted = sparkRun.sql(
      """
        | SELECT c.`Province/State`, c.`Country/Region`
        | FROM covid19confirmed c LEFT JOIN covid19recovered r ON (c.`Province/State` = r.`Province/State`)
        | LIMIT 10
    """.stripMargin
    )
    println("?")
    worldRecoveredVContracted.show()



    //Lastest Date of County's Something
    println("Total Confirmed Cases")
    val totalConfirmed = countryTotals(confirmed)
    totalConfirmed.show
    println(totalConfirmed.count())

    println("Total Confirmed Deaths")
    val totalDeaths = countryTotals(deaths)
    totalDeaths.show
    println(totalDeaths.count())

    println("Total Confirmed Recoveries")
    val totalRecovered = countryTotals(recovered)
    totalRecovered.show
    println(totalRecovered.count())

    // Active Cases = confirmed - deaths - recovered
    /*val currentInfected = totalConfirmed.crossJoin(totalDeaths)
      .select(col("totalConfirmed.Country/Region"),(col("totalConfirmed.Total") - col("totalDeaths.Total")))

     */
    //val currentInfected = spark.sql
    //currentInfected.show


  }

  def countryTotals(df : DataFrame): DataFrame ={
    val total = df.select(col("Country/Region"),col("5/2/21"))
      .groupBy("Country/Region")
      .agg(sum("5/2/21")as("Total"))
      .orderBy("Country/Region")
    total
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
