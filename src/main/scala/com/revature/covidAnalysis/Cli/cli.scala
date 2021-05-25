package com.revature.covidAnalysis.Cli

import scala.util.matching.Regex
/*
*   This CLI allows the user to interact with the Covid Analysis Application
* */
class cli {
  val commandArgPattern: Regex = "(\\w+)\\s*(.*)".r

    def printWelcome() : Unit = {
      println("This is a welcome.")
    }

}
