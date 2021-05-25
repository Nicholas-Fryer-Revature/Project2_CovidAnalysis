package com.revature.covidAnalysis

import com.revature.covidAnalysis.spark.SparkApp

object Main {
  def main(args: Array[String]): Unit = {
    val spark = new SparkApp()
    spark.sparkRun()
    spark.sparkAnalysis()
    spark.sparkClose()

  }
}