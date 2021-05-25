package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.spark.sql.{DataFrame, SparkSession}
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
  def sparkAnalysis(): Unit ={

    val userFile = sparkLoadCovidData(sparkRun())

    /*Sample Question: Which Province/State has the most deaths?*/
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

}
