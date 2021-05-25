package com.revature.covidAnalysis

import com.revature.covidAnalysis.Cli.cli

object Main {
  def main(args: Array[String]): Unit = {
    //Command Line Interface Menu
    val CmdLine = new cli();
    CmdLine.printWelcome()
  }
}