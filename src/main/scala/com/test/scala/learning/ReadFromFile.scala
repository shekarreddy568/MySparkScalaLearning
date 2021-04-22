package com.test.scala.learning

import scala.io.Source

object ReadFromFile {

  def main(args: Array[String]): Unit = {

    if(args.length > 0) {

      for(line <- Source.fromFile(args(0)).getLines())
        println(line.length + " | " + line)
    } else Console.err.println("please enter filename")
  }

}
