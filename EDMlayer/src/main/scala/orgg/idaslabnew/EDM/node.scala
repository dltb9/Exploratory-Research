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

class Node(var flist:List[(String,List[String])]) extends Logging with Serializable{
	var sel_fea:Set[String]=Set.empty  								//The selected features set
	var rem_fea:Set[String]=Set.empty++flist.map(s=>s._1).toSet		//The remain features set
	var treasureMap:Map[String,(String,String)]=Map.empty     	//The population pair of current node
	var J= new Array[Double](rem_fea.size+1) 						//The Array to track the J value of different feature size
	var k=0															//The size of the feature size

	var pre:Node=null												//The previous node	
	var children:Seq[Node]=Seq.empty    							//The children of a node
	var color:Int=0													//Denote if the node has been visited or not

	var value:Double=0.0											//The J value of current node
}

