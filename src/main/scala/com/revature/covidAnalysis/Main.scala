package com.revature.covidAnalysis

import com.revature.covidAnalysis.spark.SparkApp

object Main {
  def main(args: Array[String]): Unit = {
    //Command Line Interface Menu
    val spark = new SparkApp()
    spark.sparkRun()
    spark.sparkAnalysis()
    spark.sparkClose()

  }
}