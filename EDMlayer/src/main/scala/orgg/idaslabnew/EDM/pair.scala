package orgg.idaslabnew.EDM

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import orgg.idaslabnew.EDM
import scala.collection.mutable.Set
import scala.collection.mutable.Map
import org.idaslab.fpm._
import scala.util.control.Breaks._
import math._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import java.io._
import scala.collection.mutable.Stack
import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkException}

class pair(var value:Double,val fea:String,val pair:(String,String)=("",""),val tempMap:Map[String,(String,String)],val sel_fea:Set[String],val rem_fea:Set[String],val J:Array[Double],k:Int, var populations:Double) extends Logging with Serializable{
	var select_fea=sel_fea+fea
	var remain_fea=rem_fea-fea
	var J_new=Array.empty++J
	J_new(select_fea.size)=value
	var k_new=k+1
	//var bestmax=value
}
