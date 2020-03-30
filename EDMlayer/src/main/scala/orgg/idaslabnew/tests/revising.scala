package orgg.idaslabnew.tests

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.Set
import scala.collection.mutable.Map
import org.idaslab.fpm._
import scala.util.control.Breaks._
import math._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io._
import scala.collection.mutable.Stack
import orgg.idaslabnew.EDM._

import org.apache.log4j.Logger
import org.apache.log4j.Level

/*****
* @author Danlu
*****/

object SFFS{
	Logger.getLogger("org").setLevel(Level.OFF)
  	Logger.getLogger("akka").setLevel(Level.OFF)

	def main(args:Array[String]): Unit = {
		val conf = new SparkConf().setAppName("ExploratoryExperiment")
    	val sc = new SparkContext(conf)
		if(args.length == 6){
			val rawData = sc.textFile("file:///"+args(0)).cache()
			val edm=new edmalgorithm().setMinSupport(0.50).setMaxSupport(1.0).setpercentage(0.20).setnumofPopulation(5)
			.setFilePath(args(1)).setfea(args(2).toInt,args(3).toInt).setpreCon(args(4).toDouble,args(5).toDouble).rundata(rawData)
			//.setFilePath("/home/dltb9/data/syntestmix0005percent2/").rundata(rawData)
		}
		else{
			println("Expected input parameters: <filePath>")
		}

	}
}
