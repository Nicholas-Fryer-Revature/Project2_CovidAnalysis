package com.revature.covidAnalysis.spark

import directories.hdfsLocation
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, desc, struct, sum, udf}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
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

    //ANALYSIS QUESTION 1
    //Latest Date of County's Stats
    val totalConfirmed = countryTotals(confirmed)
    val totalDeaths = countryTotals(deaths)
    val totalRecovered = countryTotals(recovered)

    //Views of Totals Dataframes
    totalConfirmed.createOrReplaceTempView("vTotalConfirmed")
    totalDeaths.createOrReplaceTempView("vTotalDeaths")
    totalRecovered.createOrReplaceTempView("vTotalRecovered")

    // Active Cases = confirmed - deaths - recovered
    val currentInfected = sparkRun().sql(
      """
        |SELECT c.Country,
        |c.Total - d.Total - r.Total as Infected
        |From (vTotalConfirmed c
        |JOIN vTotalDeaths d ON c.Country == d.Country)
        |JOIN vTotalRecovered r ON c.Country == r.Country
        |ORDER BY Infected DESC
        |""".stripMargin)

    //Analysis Question 1A
    println("Top 10 Most Infected Countries Right Now")
    currentInfected.orderBy(col("Infected").desc).limit(10).show()

    //Analysis Question 1B
    println("Top 10 Least Infected Countries Right Now")
    currentInfected.filter("Infected > 1").orderBy(col("Infected").asc).limit(10).show()

    //ANALYSIS QUESTION TWO
    //compare monthly confirmations to recovery; negative values indicate more
    //recoveries than contractions that month
    println("How was the world recovery effort by region?")
    val worldRecoveryStats = conformedRecoveredWorld(sparkRun(), confirmed, recovered )
    worldRecoveryStats.show()

    worldRecoveryStats.createOrReplaceTempView("worldrecovery")
    println("Asia:")
    val recoveryAsia = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "China"
        |OR `Country/Region` like "India"
        |OR `Country/Region` like "Indonesia"
        |OR `Country/Region` like "Japan"
        |OR `Country/Region` like "Nepal"
        |OR `Country/Region` like "Malaysia"
        |OR `Country/Region` like "Nepal"
        |OR `Country/Region` like "Russia"
        |OR `Country/Region` like "Syria"
        |OR `Country/Region` like "Singapore"
        |OR `Country/Region` like "Thailand"
        |OR `Country/Region` like "Taiwan"
        |OR `Country/Region` like "Vietnam"
        |OR `Country/Region` like "Uzbekistan"
        |""".stripMargin
    )
    recoveryAsia.orderBy(desc("Progress_by_April")).orderBy(desc("Progress_by_April")).show()
    //recoveryAsia.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/Asia.csv")
    println("MidEast:")
    val recoveryMidEast = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "Afghanistan"
        |OR `Country/Region` like "Armenia"
        |OR `Country/Region` like "Iran"
        |OR `Country/Region` like "Iraq"
        |OR `Country/Region` like "Jordan"
        |OR `Country/Region` like "Kazakhstan"
        |OR `Country/Region` like "Lebanon"
        |OR `Country/Region` like "Pakistan"
        |OR `Country/Region` like "Saudi Arabia"
        |OR `Country/Region` like "Emirates%"
        |""".stripMargin
    )
    recoveryMidEast.orderBy(desc("Progress_by_April")).show()
    //recoveryMidEast.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/MidEast.csv")
    println("Europe:")
    val recoveryEurope = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "Albania"
        |OR `Country/Region` like "Spain"
        |OR `Country/Region` like "Austria"
        |OR `Country/Region` like "Belgium"
        |OR `Country/Region` like "Sweden"
        |OR `Country/Region` like "Croatia"
        |OR `Country/Region` like "Czechia"
        |OR `Country/Region` like "Denmark"
        |OR `Country/Region` like "Estonia"
        |OR `Country/Region` like "Finland"
        |OR `Country/Region` like "France"
        |OR `Country/Region` like "Germany"
        |OR `Country/Region` like "Greece"
        |OR `Country/Region` like "Hungary"
        |OR `Country/Region` like "Iceland"
        |OR `Country/Region` like "Ireland"
        |OR `Country/Region` like "Italy"
        |OR `Country/Region` like "Lithuania"
        |OR `Country/Region` like "Netherlands"
        |OR `Country/Region` like "Romania"
        |OR `Country/Region` like "Portugal"
        |OR `Country/Region` like "Poland"
        |OR `Country/Region` like "United Kingdom"
        |OR `Country/Region` like "Ukraine"
        |""".stripMargin
    )
    recoveryEurope.orderBy(desc("Progress_by_April")).show()
    //recoveryEurope.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/Europe.csv")
    println("Africa:")
    val recoveryAfrica = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "Algeria"
        |OR `Country/Region` like "Botswana"
        |OR `Country/Region` like "Cameroon"
        |OR `Country/Region` like "Cape Verde"
        |OR `Country/Region` like "Chad"
        |OR `Country/Region` like "Comoros"
        |OR `Country/Region` like "Congo"
        |OR `Country/Region` like "Djibouti"
        |OR `Country/Region` like "Egypt"
        |OR `Country/Region` like "Ethiopia"
        |OR `Country/Region` like "Gambia"
        |OR `Country/Region` like "Kenya"
        |OR `Country/Region` like "Guinea"
        |OR `Country/Region` like "Liberia"
        |OR `Country/Region` like "Libya"
        |OR `Country/Region` like "Lesotho"
        |OR `Country/Region` like "Madagascar"
        |OR `Country/Region` like "Malawi"
        |OR `Country/Region` like "Mali"
        |OR `Country/Region` like "Morocco"
        |OR `Country/Region` like "Namibia"
        |OR `Country/Region` like "Niger"
        |OR `Country/Region` like "Nigeria"
        |OR `Country/Region` like "Rwanda"
        |OR `Country/Region` like "Senegal"
        |OR `Country/Region` like "Somalia"
        |OR `Country/Region` like "South Africa"
        |OR `Country/Region` like "Tanzania"
        |OR `Country/Region` like "Tunisia"
        |OR `Country/Region` like "Uganda"
        |OR `Country/Region` like "Zambia"
        |OR `Country/Region` like "Zimbabwe"
        |""".stripMargin
    )
    recoveryAfrica.orderBy(desc("Progress_by_April")).show()
    //recoveryAfrica.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/Africa.csv")
    println("North America:")
    val recoveryNA = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "Canada"
        |OR `Country/Region` like "America"
        |OR `Country/Region` like "Mexico"
        |OR `Country/Region` like "Belize"
        |OR `Country/Region` like "Bahamas"
        |OR `Country/Region` like "Barbados"
        |OR `Country/Region` like "Bermuda"
        |OR `Country/Region` like "Virgin"
        |OR `Country/Region` like "Cayman"
        |OR `Country/Region` like "Costa Rica"
        |OR `Country/Region` like "Cuba"
        |OR `Country/Region` like "Dominican Republic"
        |OR `Country/Region` like "El Salvador"
        |OR `Country/Region` like "Guadeloupe"
        |OR `Country/Region` like "Guatemala"
        |OR `Country/Region` like "Haiti"
        |OR `Country/Region` like "Honduras"
        |OR `Country/Region` like "Honduras"
        |OR `Country/Region` like "Jamaica"
        |OR `Country/Region` like "Nicaragua"
        |OR `Country/Region` like "Panama"
        |OR `Country/Region` like "Puerto"
        |OR `Country/Region` like "Saint"
        |OR `Country/Region` like "Virgin"
        |""".stripMargin
    )
    recoveryNA.orderBy(desc("Progress_by_April")).show()
    //recoveryNA.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/NA.csv")
    println("South America:")
    val recoverySA = sparkRun.sql(
      """
        |SELECT *
        |FROM worldrecovery
        |WHERE `Country/Region` like "Brazil"
        |OR `Country/Region` like "Argentina"
        |OR `Country/Region` like "Bolivia"
        |OR `Country/Region` like "Chile"
        |OR `Country/Region` like "Columbia"
        |OR `Country/Region` like "Ecuador"
        |OR `Country/Region` like "French Guiana"
        |OR `Country/Region` like "Guyana"
        |OR `Country/Region` like "Paraguay"
        |OR `Country/Region` like "Peru"
        |OR `Country/Region` like "Suriname"
        |OR `Country/Region` like "Uruguay"
        |OR `Country/Region` like "Venezuela"
        |""".stripMargin
    )
    recoverySA.orderBy(desc("Progress_by_April")).show()
    //recoverySA.select(col("Country/Region"), col("Progress_by_April")).coalesce(1).write.csv(hdfsLocation.hdfs_path + "output/SA.csv")

    //ANALYSIS QUESTION THREE!
    println("Death rate of COVID19 in the United States (#ofDeaths/#ofConfirmed)")
    confirmedUSvsDeathsUS(sparkRun(), confirmedUS, deathsUS).show(51)

  }

  def conformedRecoveredWorld(spark: SparkSession, confirmedWorld: DataFrame, recoveredWorld: DataFrame ): DataFrame = {
    //from the raw confirmed cases data,
    // select only the country name and last day of each month
    //group by country name so that any country with provinces/states are summed together
    //second select to isolate the rollover numbers for each month
    val confirmedWorld2 = confirmedWorld.select(
      "Country/Region",
      "1/31/20",
      "2/29/20",
      "3/31/20",
      "4/30/20",
      "5/31/20",
      "6/30/20",
      "7/31/20",
      "8/31/20",
      "9/30/20",
      "10/31/20",
      "11/30/20",
      "12/31/20",
      "1/31/21",
      "2/28/21",
      "3/31/21",
      "4/30/21"
    ).groupBy("Country/Region").agg(
      sum("1/31/20").as("Jan20"),
      sum("2/29/20").as("Feb20"),
      sum("3/31/20").as("Mar20"),
      sum("4/30/20").as("Apr20"),
      sum("5/31/20").as("May20"),
      sum("6/30/20").as("Jun20"),
      sum("7/31/20").as("Jul20"),
      sum("8/31/20").as("Aug20"),
      sum("9/30/20").as("Sep20"),
      sum("10/31/20").as("Oct20"),
      sum("11/30/20").as("Nov20"),
      sum("12/31/20").as("Dec20"),
      sum("1/31/21").as("Jan21"),
      sum("2/28/21").as("Feb21"),
      sum("3/31/21").as("Mar21"),
      sum("4/30/21").as("Apr21"),
    ).selectExpr("`Country/Region`",
      "Jan20",
      "Feb20 - Jan20 as Feb20",
      "Mar20 - Feb20 as Mar20",
      "Apr20 - Mar20 as Apr20",
      "May20 - Apr20 as May20",
      "Jun20 - May20 as Jun20",
      "Jul20 - Jun20 as Jul20",
      "Aug20 - Jul20 as Aug20",
      "Sep20 - Aug20 as Sep20",
      "Oct20 - Sep20 as Oct20",
      "Nov20 - Oct20 as Nov20",
      "Dec20 - Nov20 as Dec20",
      "Jan21 - Dec20 as Jan21",
      "Feb21 - Jan21 as Feb21",
      "Mar21 - Feb21 as Mar21",
      "Apr21 - Mar21 as Apr21"
    )


    //from the raw recovery data,
    //select only the country name and last day of each month
    //group by country name so that any country with provinces/states are summed together
    //second select to isolate the rollover numbers for each month
    val recoveredWorld2 = recoveredWorld.select(
      "Country/Region",
      "1/31/20",
      "2/29/20",
      "3/31/20",
      "4/30/20",
      "5/31/20",
      "6/30/20",
      "7/31/20",
      "8/31/20",
      "9/30/20",
      "10/31/20",
      "11/30/20",
      "12/31/20",
      "1/31/21",
      "2/28/21",
      "3/31/21",
      "4/30/21"
    ).groupBy("Country/Region").agg(
      sum("1/31/20").as("Jan20"),
      sum("2/29/20").as("Feb20"),
      sum("3/31/20").as("Mar20"),
      sum("4/30/20").as("Apr20"),
      sum("5/31/20").as("May20"),
      sum("6/30/20").as("Jun20"),
      sum("7/31/20").as("Jul20"),
      sum("8/31/20").as("Aug20"),
      sum("9/30/20").as("Sep20"),
      sum("10/31/20").as("Oct20"),
      sum("11/30/20").as("Nov20"),
      sum("12/31/20").as("Dec20"),
      sum("1/31/21").as("Jan21"),
      sum("2/28/21").as("Feb21"),
      sum("3/31/21").as("Mar21"),
      sum("4/30/21").as("Apr21"),
    ).orderBy("Country/Region").selectExpr("`Country/Region`",
      "Jan20",
      "Feb20 - Jan20 as Feb20",
      "Mar20 - Feb20 as Mar20",
      "Apr20 - Mar20 as Apr20",
      "May20 - Apr20 as May20",
      "Jun20 - May20 as Jun20",
      "Jul20 - Jun20 as Jul20",
      "Aug20 - Jul20 as Aug20",
      "Sep20 - Aug20 as Sep20",
      "Oct20 - Sep20 as Oct20",
      "Nov20 - Oct20 as Nov20",
      "Dec20 - Nov20 as Dec20",
      "Jan21 - Dec20 as Jan21",
      "Feb21 - Jan21 as Feb21",
      "Mar21 - Feb21 as Mar21",
      "Apr21 - Mar21 as Apr21"
    )

    //create views for confirmed and recovered cases to query on
    confirmedWorld2.createOrReplaceTempView("confirmed")
    recoveredWorld2.createOrReplaceTempView("recovered")
    //join both tables on Country/Region, subtract the data from corresponding columns
    //positive values indicate more confirmations than recoveries that month
    //negative values indicate more recoveries than confirmations that month
    val progress = spark.sql(
      """
        | SELECT c.`Country/Region`,
        | c.Jan20 - r.Jan20 as Jan20_rc,
        | c.Feb20 - r.Feb20 as Feb20_rc,
        | c.Mar20 - r.Mar20 as Mar20_rc,
        | c.Apr20 - r.Apr20 as Apr20_rc,
        | c.May20 - r.May20 as May20_rc,
        | c.Jun20 - r.Jun20 as Jun20_rc,
        | c.Jul20 - r.Jul20 as Jul20_rc,
        | c.Aug20 - r.Aug20 as Aug20_rc,
        | c.Sep20 - r.Sep20 as Sep20_rc,
        | c.Oct20 - r.Oct20 as Oct20_rc,
        | c.Nov20 - r.Nov20 as Nov20_rc,
        | c.Dec20 - r.Dec20 as Dec20_rc,
        | c.Jan21 - r.Jan21 as Jan21_rc,
        | c.Feb21 - r.Feb21 as Feb21_rc,
        | c.Mar21 - r.Mar21 as Mar21_rc,
        | c.Apr21 - r.Apr21 as Apr21_rc
        | FROM confirmed c
        | JOIN recovered r WHERE c.`Country/Region` == r.`Country/Region`
        | ORDER BY `Country/Region`
        |""".stripMargin
    ).withColumn("Progress_by_April",
      col("Jan20_rc") +
        col("Feb20_rc") +
        col("Mar20_rc") +
        col("Apr20_rc") +
        col("May20_rc") +
        col("Jun20_rc") +
        col("Jul20_rc") +
        col("Aug20_rc") +
        col("Sep20_rc") +
        col("Oct20_rc") +
        col("Nov20_rc") +
        col("Dec20_rc") +
        col("Jan21_rc") +
        col("Feb21_rc") +
        col("Mar21_rc") +
        col("Apr21_rc")
    )

    //    progress.orderBy("Progress_by_April")
    progress
  }

  def countryTotals(df : DataFrame): DataFrame ={
    val total = df.select(col("Country/Region")as "Country",col("5/2/21"))
      .groupBy("Country")
      .agg(sum("5/2/21")as("Total"))
      .orderBy("Country")
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
/* confirmedUSvsDeathsUS will take the SparkSession variable, confirmedUS and deathsUS dataframes to output a
* dataframe joining the two. This will allow us to see
*/
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
    //create an avg column of the weeks, exclude month 1 and  month 2 all null data.
    val percentage3 = percentage2.withColumn("Average", average(
      struct($"Week3", $"Week4", $"Week5", $"Week6", $"Week8", $"Week9",
              $"Week10", $"Week11", $"Week12", $"Week13", $"Week14", $"Week15", $"Week16")
    ))

    //Convert percentage to 3 decimal points and rename Week to month/year
    val percentage4 = percentage3.selectExpr(
      "Province_State AS STATE",
      "CAST(Average as Decimal(4,3)) AS AVG",
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
    ).orderBy(desc("AVG"))
    percentage4
  }
}
