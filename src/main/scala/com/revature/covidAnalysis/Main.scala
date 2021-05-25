package com.revature.covidAnalysis

import com.revature.covidAnalysis.Cli.cli
import com.revature.covidAnalysis.spark.SparkApp

object Main {
  def main(args: Array[String]): Unit = {
    //Command Line Interface Menu
    val CmdLine = new cli()
    CmdLine.printWelcome()
    val spark = new SparkApp()
    spark.sparkRun()
    spark.sparkTest()
    spark.sparkClose()

  }
}