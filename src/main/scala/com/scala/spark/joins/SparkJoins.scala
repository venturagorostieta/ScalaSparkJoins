package com.scala.spark.joins

import org.apache.spark._
import org.apache.spark.SparkContext._

/**
 * join using 2 csv files: persons and address details
 * Spark API with Scala 
 */
object SparkJoins {
  
  /**
   * argument[0] will be input file for user details file.
   * argument[1] will be input file for address details file. 
   * argument[2] will be output path for saving Spark Joins output.
   */
    def main(args: Array[String]) 
   {
     
    
      val sparkContext = new SparkContext("local","Spark Joins",
          System.getenv("SPARK_HOME"))
      
      /*Reading input from HDFS , Persons*/
      val userInputRDD = sparkContext.textFile(args(0))
      
      /*
       *KEY :: UserID  
       *VALUE :: <FirstName,LastName> */
      val userPairs = userInputRDD.map(line => (line.split(",")(0), line.split(",")(1)+","+line.split(",")(2)))
      
      /*Reading input from Address Data File*/
      val addressInputRDD = sparkContext.textFile(args(1))
      
      /*
       *KEY :: AddressID  
       *VALUE :: <City,State,Country> */
      val addressPairs = addressInputRDD.map(line => (line.split(",")(0), line.split(",")(1)+","+line.split(",")(2)+","+line.split(",")(3)))
      
      /*Default Join operation (Inner join)*/
      val innerJoinResult = userPairs.join(addressPairs);
      
      innerJoinResult.saveAsTextFile(args(2)+"/InnerJoin")
      
      /*Left Outer join operation*/
      val leftOuterJoin = userPairs.leftOuterJoin(addressPairs)
      
      /*Storing values of Left Outer join*/
      leftOuterJoin.saveAsTextFile(args(2)+"/LeftOuterJoin")
      
      /*Right Outer join operation*/
      val rightOuterJoin = userPairs.rightOuterJoin(addressPairs)
      
      /*Storing values of Right Outer join*/
      rightOuterJoin.saveAsTextFile(args(2)+"/RightOuterJoin")
      
      sparkContext.stop();
      
    }
  
}