package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{avg, col, desc, expr, monotonically_increasing_id, struct, sum, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.util.Try

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
    /*Sample Question: How did the US recover compared to contraction
    (time_series_US confirmed vs time_series_covid recovered)*/
    confirmedUS.createOrReplaceTempView("covid19confirmedUS")
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

    //ANALYSIS QUESTION THREE!
    println("Death rate of COVID19 in the United States (#ofDeaths/#ofConfirmed)")
    confirmedUSvsDeathsUS(sparkRun(), confirmedUS, deathsUS).show()
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

  def confirmedUSvsDeathsUS(spark: SparkSession, confirmedUS : DataFrame, deathsUS : DataFrame): DataFrame ={
    import spark.implicits._

    //select the state name and last day of each month excluding May 2021
    val confirmedUS2 = confirmedUS.select(
      col("Province_State"),
      col("1/31/20"),
      col("2/29/20"),
      col("3/31/20"),
      col("4/30/20"),
      col("5/31/20"),
      col("6/30/20"),
      col("7/31/20"),
      col("8/31/20"),
      col("9/30/20"),
      col("10/31/20"),
      col("11/30/20"),
      col("12/31/20"),
      col("1/31/21"),
      col("2/28/21"),
      col("3/31/21"),
      col("4/30/21"),
    )

    //aggregate sum of all states, rename dates to week for easier typing
    val confirmedUS3 = confirmedUS2.groupBy($"Province_State").agg(sum($"1/31/20") as ("Week1"),
          sum($"2/29/20") as "Week2" ,sum($"3/31/20").as("Week3") ,sum($"4/30/20").as("Week4")
          ,sum("5/31/20").as("Week5") ,sum("6/30/20").as("Week6")
          ,sum("7/31/20").as("Week7") ,sum("8/31/20").as("Week8")
          ,sum("9/30/20").as("Week9") ,sum("10/31/20").as("Week10")
          ,sum("11/30/20").as("Week11") ,sum("12/31/20").as("Week12")
          ,sum("1/31/21").as("Week13") ,sum("2/28/21").as("Week14")
          ,sum("3/31/21").as("Week15") ,sum("4/30/21").as("Week16")).orderBy($"Province_State")

    //subtract the previous week from the current week sum
    val confirmedUS4 = confirmedUS3.selectExpr(
      "Province_State",
      "Week1",
      "Week2 -  Week1  as Week2",
      "Week3  - Week2  as Week3",
      "Week4  - Week3  as Week4",
      "Week5  - Week4  as Week5",
      "Week6  - Week5  as Week6",
      "Week7  - Week6  as Week7",
      "Week8  - Week7  as Week8",
      "Week9  - Week8  as Week9",
      "Week10 - Week9  as Week10",
      "Week11 - Week10 as Week11",
      "Week12 - Week11 as Week12",
      "Week13 - Week12 as Week13",
      "Week14 - Week13 as Week14",
      "Week15 - Week14 as Week15",
      "Week16 - Week15 as Week16"
    )
    //select the state name and last day of each month excluding May 2021
    val deathsUS2 = deathsUS.select(
      col("Province_State"),
      col("1/31/20"),
      col("2/29/20"),
      col("3/31/20"),
      col("4/30/20"),
      col("5/31/20"),
      col("6/30/20"),
      col("7/31/20"),
      col("8/31/20"),
      col("9/30/20"),
      col("10/31/20"),
      col("11/30/20"),
      col("12/31/20"),
      col("1/31/21"),
      col("2/28/21"),
      col("3/31/21"),
      col("4/30/21"),
    )

    //aggregate sum of all states, rename dates to week for easier typing
    val deathsUS3 = deathsUS2.groupBy($"Province_State").agg(sum($"1/31/20") as ("Week1"),
      sum($"2/29/20") as "Week2" ,sum($"3/31/20").as("Week3") ,sum($"4/30/20").as("Week4")
      ,sum("5/31/20").as("Week5") ,sum("6/30/20").as("Week6")
      ,sum("7/31/20").as("Week7") ,sum("8/31/20").as("Week8")
      ,sum("9/30/20").as("Week9") ,sum("10/31/20").as("Week10")
      ,sum("11/30/20").as("Week11") ,sum("12/31/20").as("Week12")
      ,sum("1/31/21").as("Week13") ,sum("2/28/21").as("Week14")
      ,sum("3/31/21").as("Week15") ,sum("4/30/21").as("Week16")).orderBy($"Province_State")

    //subtract the previous week from the current week sum
    val deathsUS4 = deathsUS3.selectExpr(
      "Province_State",
      "Week1",
      "Week2 -  Week1  as Week2",
      "Week3  - Week2  as Week3",
      "Week4  - Week3  as Week4",
      "Week5  - Week4  as Week5",
      "Week6  - Week5  as Week6",
      "Week7  - Week6  as Week7",
      "Week8  - Week7  as Week8",
      "Week9  - Week8  as Week9",
      "Week10 - Week9  as Week10",
      "Week11 - Week10 as Week11",
      "Week12 - Week11 as Week12",
      "Week13 - Week12 as Week13",
      "Week14 - Week13 as Week14",
      "Week15 - Week14 as Week15",
      "Week16 - Week15 as Week16"
    )
      confirmedUS4.createOrReplaceTempView("confirmedUS")
      deathsUS4.createOrReplaceTempView("deathsUS")

    //JOIN the two dataframes, create percentage of deaths/confirmed cases
    val percentage = spark.sql(
      """
        | SELECT d.Province_State,
        | d.Week1/c.Week1 as Week1,
        | d.Week2/c.Week2 as Week2,
        | d.Week3/c.Week3 as Week3,
        | d.Week4/c.Week4 as Week4,
        | d.Week5/c.Week5 as Week5,
        | d.Week6/c.Week6 as Week6,
        | d.Week7/c.Week7 as Week7,
        | d.Week8/c.Week8 as Week8,
        | d.Week9/c.Week9 as Week9,
        | d.Week10/c.Week10 as Week10,
        | d.Week11/c.Week11 as Week11,
        | d.Week12/c.Week12 as Week12,
        | d.Week13/c.Week13 as Week13,
        | d.Week14/c.Week14 as Week14,
        | d.Week15/c.Week15 as Week15,
        | d.Week16/c.Week16 as Week16
        | from confirmedUS c
        | JOIN deathsUS d WHERE c.Province_State == d.Province_State ORDER BY d.Province_State
    """.stripMargin
    )

    //filter out the non-states
    val percentage2 = percentage
      .filter("Province_State != 'American Samoa'")
      .filter("Province_State != 'Diamond Princess'")
      .filter("Province_State != 'Grand Princess'")
      .filter("Province_State != 'Guam'")
      .filter("Province_State != 'Virgin Islands'")
      .filter("Province_State != 'Puerto Rico'")
      .filter("Province_State != 'Northern Mariana Islands'")

    //UDF for average
    def average = udf((row: Row) => {
      val values = row.toSeq.map(x => Try(x.toString.toDouble).toOption).filter(_.isDefined).map(_.get)
      if(values.nonEmpty) values.sum / values.length else 0.0
    })
    //create an avg column of the weeks
    val percentage3 = percentage2.withColumn("Average", average(
      struct($"Week1", $"Week2", $"Week3", $"Week4", $"Week5", $"Week6", $"Week8", $"Week9",
              $"Week10", $"Week11", $"Week12", $"Week13", $"Week14", $"Week15", $"Week16")
    ))

    //Convert percentage to 3 decimal points and rename Week to month/year
    val percentage4 = percentage3.selectExpr(
      "Province_State AS STATE",
      "CAST(Average as Decimal(4,3)) AS AVG",
      "CAST(Week1 as Decimal(4,3)) AS JAN20",
      "CAST(Week2 as Decimal(4,3)) AS FEB20",
      "CAST(Week3 as Decimal(4,3)) AS MAR20",
      "CAST(Week4 as Decimal(4,3)) AS APR20",
      "CAST(Week5 as Decimal(4,3)) AS MAY20",
      "CAST(Week6 as Decimal(4,3)) AS JUN20",
      "CAST(Week7 as Decimal(4,3)) AS JUL20",
      "CAST(Week8 as Decimal(4,3)) AS AUG20",
      "CAST(Week9 as Decimal(4,3)) AS SEP20",
      "CAST(Week10 as Decimal(4,3)) AS OCT20",
      "CAST(Week11 as Decimal(4,3)) AS NOV20",
      "CAST(Week12 as Decimal(4,3)) AS DEC20",
      "CAST(Week13 as Decimal(4,3)) AS JAN21",
      "CAST(Week14 as Decimal(4,3)) AS FEB21",
      "CAST(Week15 as Decimal(4,3)) AS MAR21",
      "CAST(Week16 as Decimal(4,3)) AS APR21",
    )
    percentage4.orderBy(desc("AVG"))
  }
}
