package com.aero.core

import com.blackeyetech.sj.scalaspark.App;

object ScalaSample extends App{
  def main(args: Array[String]) {
    val aggString =  """("cols"   ->   ("statecode", "policyID")  , "aggCond"     ->      
      (("max", "columnA", "maxColumnA"),("max", "columnB", "maxColumnB") ) ) """
    val keyPattern = """\"\w+\"\s+\-\>""".r
    //val n1 = """\(\"\w+\"\s+?\,\s+?\"\w+\"\s+?\,\s+?\"\w+\"\s?""".r
    val gKeys = keyPattern.findAllIn(aggString).toList 
    
    gKeys.foreach(println)
    val string1 = aggString.split(gKeys(1)).toList
    val aggCond = "List" + string1(1).trim.init.trim.replaceAll("\\),", "\\),\n")
    println(aggCond)
    val string2 = string1(0).split(gKeys(0))
    val aggCols = "List" + (string2(1).trim.init.trim)
    println(aggCols)
    
  }

}