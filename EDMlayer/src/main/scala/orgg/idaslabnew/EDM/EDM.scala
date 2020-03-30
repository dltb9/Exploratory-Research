package orgg.idaslabnew.EDM

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

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
import org.apache.spark.{HashPartitioner, Logging, Partitioner, SparkException}
import scala.collection.mutable.ListBuffer

//fourteen feature million new
class edmalgorithm extends Logging with Serializable{
	var minSupport:Double=0.5
	var numofPopulation:Int=1
	var FilePath:String="./"
	var flist:List[(String,List[String])]=List.empty
	var numofFeature:Int=1
	var datasize:Long=0
	var sqlContext:org.apache.spark.sql.SQLContext=null
	var percentage:Double=0.05
	var maxlength:Int=3

	var maxSupport:Double=0.95

	var wholepop=0.0
	var wholevalue=0.0
	var countNode=0

	var feaSet:Set[String]=Set.empty
	var startfea=0
	var endfea=0

	//componnet time sum variable
	var sum1=0.0
	var sum2=0.0
	var sum3=0.0
	var sum4=0.0
	var sum5=0.0
	var sum6=0.0
	var sum7=0.0
	var sum8=0.0
	var sum9=0.0
	var sum10=0.0
	var sum11=0.0
	var sum12=0.0
	var sum13=0.0
	var sum14=0.0
	var sum15=0.0
	var sum16=0.0
	var sum17=0.0
	var sum18=0.0
	var sum19=0.0


	/**
	* @param maxlength of Apriori
	* @return
	*/
	def setmaxlength(maxlength:Int):this.type={
		this.maxlength=maxlength
		this
	}

	/**
	* @param percentage
	* @return
	*/
	def setpercentage(percentage:Double):this.type={
		this.percentage=percentage
		this
	}

	/**
	* @param minSupport
	* @return
	*/
	def setMinSupport(minSupport:Double):this.type={
		this.minSupport=minSupport
		this
	}

	/**
	* @param maxSupport
	* @return
	*/
	def setMaxSupport(maxSupport:Double):this.type={
		this.maxSupport=maxSupport
		this
	}

	/**
	* @param number of Population features
	* @return
	*/
	def setnumofPopulation(numofPopulation:Int):this.type={
		this.numofPopulation=numofPopulation
		this
	}

	/**
	* @param directory to put the files
	* @return
	*/
	def setFilePath(FilePath:String):this.type={
		this.FilePath=FilePath
		this
	}

	/**
	* @param rawData
	* @return
	*/
	def setfeaturelist(rawData:RDD[String]):this.type={
		this.numofFeature=getnumofFeature(rawData)
		val returnvalue=preprocessing(rawData)
		this.flist=returnvalue._1
		this.datasize=returnvalue._2
		this.sqlContext=returnvalue._3
		this
	}


	def setpreCon(wholepop:Double,wholevalue:Double):this.type={
		this.wholepop=wholepop
		this.wholevalue=wholevalue
		this
	}


	def setfea(startfea:Int,endfea:Int):this.type={
		this.startfea=startfea
		this.endfea=endfea
		val sset=Set.empty++=(startfea to endfea).map(a=>"P"+a).toSet
		this.feaSet=sset
		this
	}

	/**
	* @param option to choose the function
	* @return paris to be selected
	*/
	def composePairs(option:Int,nums:Seq[String]):Seq[(String, String)]={
		val pairs=option match {
			case 1 => nums.flatMap(x => nums.map(y => (x,y)))
			case 2 => nums.flatMap(x => nums.map(y => (x,y))).map{case (a, b) => if(a > b) (a, b) else (b, a)}.distinct
			case 3 => nums.flatMap(x => nums.map(y => (x,y))).filter(s=>s._1==s._2)
			case 4 => nums.flatMap(x => nums.map(y => (x,y))).filter(s=>s._1!=s._2).map{case (a, b) => if(a > b) (a, b) else (b, a)}.distinct
			case _ => nums.flatMap(x => nums.map(y => (x,y)))
		}
		pairs
	}

	/**
	* @param raw data
	* @return number of features
	*/
	def getnumofFeature(rawData:RDD[String]):Int={
		val numofFeature=rawData.map(_.split(",",-1)).map(a=>a.size).max
		numofFeature
	}

	/**
	* @param raw data, number of Population features
	* @return list of Population and their distinct values
	*/
	def preprocessing(rawData:RDD[String]):(List[(String,List[String])],Long,org.apache.spark.sql.SQLContext)={
		val schemaString =(1 to numofFeature).toList.map(s=>"P"+s)
		val schema=StructType(schemaString.map(fieldName => StructField(fieldName, StringType, true)))
		val rowRDDold = rawData.map(_.split(",",-1).map(_.replaceAll("\\s","")))
		val maxLength=rowRDDold.map(a=>a.size).max
		val rowRDD=rowRDDold.map(a=>{
			var b=a
			val cha=maxLength-a.size
			for(i<-1 to cha){
				b=b:+""
			}
			b
		}).map(s => Row(s:_*))
		import sqlContext.implicits

		val sqlContext=new org.apache.spark.sql.SQLContext(rawData.context)
		val Testclass = sqlContext.createDataFrame(rowRDD, schema)
		Testclass.registerTempTable("testTable")
		//cache table
		sqlContext.cacheTable("testTable")
		val results = sqlContext.sql("select * from testTable")

		val t=results.schema.fields.map(s=>(s.name,s.dataType.toString))
		var flist:List[(String,List[String])]=Nil
		for(n<-t){ 
			val k=n._1; 
			if(t.take(numofPopulation).map(_._1).contains(k))
			{
				val r = sqlContext.sql(s"select distinct ($k) from testTable"); 
				val p=(n._1,(r.map(s=>if (n._2=="IntegerType") s.getInt(0).toString else s.getString(0)).collect.filter(_!="")).toList)
				flist=p::flist
			}          
		}


		(flist,results.count,sqlContext)
	}

	/**
	* @param tempMap is the temporal population Map, LorR represents if it is left or right
	* @return J(g-index) value is returned
	*/
	def separateQuery(tempMap:Map[String,(String,String)],LorR:String) = {
		val qian=(numofPopulation+1 to numofFeature).toList.map(s=>"P"+s).mkString(",")
		// val qian=(numofPopulation+1 to numofFeature).toList.map(s=>"P").mkString(",")
		val query=if(LorR=="left") tempMap.map(s=>(s._1+"='"+s._2._1+"'")).mkString(" and ")
				  else tempMap.map(s=>(s._1+"='"+s._2._2+"'")).mkString(" and ")
		val result=sqlContext.sql(s"select $qian from testTable where $query")
		val w=result.columns
		result.map(s=>(s.toSeq.toArray zip w).filter{case(a,b)=>a!=""} map {case (a,b)=>b+"->"+a})
		//result.map(s=>(s.toSeq.toArray zip w).filter{case(a,b)=>a!=""} map {case (a,b)=>a.toString})
	}

	def subsetfun(input:(org.idaslab.fpm.ContrastItem[String],List[org.idaslab.fpm.ContrastItem[String]])): Boolean = {
		var k=true
		for(e<-input._2){ 
			if(input._1.item.subsetOf(e.item) && (e.item-- input._1.item).nonEmpty && ((input._1.class1.support-e.class1.support).abs<=0.05 && (input._1.class2.support-e.class2.support).abs<=0.05)) k=false
		}
		k
	}

	/**
	* @param left and right means two separate population, minSupport and temporal file name
	* @return list of Population and their distinct values
	*/
	def conTrast(left:RDD[Array[String]],right:RDD[Array[String]],minSupport:Double,tempMapname:String)={
		val t28 = System.nanoTime

		val data0=left.cache()
		val data1=right.cache()


  		val hpgrowthleft = new HPGrowth().setTopKPercent(0.05).setSplitThreshold(1000000000).setMinSupport(minSupport).setMaxSupport(maxSupport)
		val allfrequent_left=hpgrowthleft.getAllFI(data0)
		val frequent_first=allfrequent_left.map(s=>{val a=new FreqItemset(new Itemset(s._1.toSet),s._2.toLong); a})

		val hpgrowthright = new HPGrowth().setTopKPercent(0.05).setSplitThreshold(1000000000).setMinSupport(minSupport).setMaxSupport(maxSupport)
		val allfrequent_right=hpgrowthright.getAllFI(data1)
		val frequent_second=allfrequent_right.map(s=>{val a=new FreqItemset(new Itemset(s._1.toSet),s._2.toLong); a})
                  
                println("frequent patterns here!")
		val t29 = System.nanoTime

		sum11=sum11+(t29-t28)/1e9d;

		val t30 = System.nanoTime

      	var conModel = new Contrast(frequent_first,data0.count,frequent_second,data1.count).run().growth(data0,data1).lift().supportDifference()
      	
		val temp=conModel.contrastItems.filter(row=>row.class1.growth>2 || row.class2.growth>2).map(r=>if(r.class1.class_name=="class2") {val temp=r.class1;r.class1=r.class2;r.class2=temp;r} else r).cache()
	
		val w=temp.filter(a=>a.item.size<=20).collect

		temp.unpersist()
		val t31 = System.nanoTime

		sum12=sum12+(t31-t30)/1e9d;


		val t32 = System.nanoTime

		val broadcasted = data0.context.broadcast(w.toList)
		val rdd=data0.context.parallelize(w)
		val remainset=rdd.map(i => (i,subsetfun(i,broadcasted.value))).filter(_._2==true).map(_._1).collect()
		broadcasted.destroy()

		val m=remainset.map(row=>(row.item,if(row.class1.growth.isInfinity||row.class2.growth.isInfinity) 100 else if(row.class1.growth>=row.class2.growth) tanh(row.class1.growth/100)*100 else tanh(row.class2.growth/100)*100))

		val t=m.sortBy(x=>(-x._2,-x._1.size)).zipWithIndex.map { case (line, i) => (i+1,line)}
		val filename=FilePath+tempMapname+".txt"
		val patternfile = new FileWriter(filename, true);
		val ww=remainset.sortBy(x=>(-x.class1.support,-x.class2.support))
		
		val t33 = System.nanoTime

		sum13=sum13+(t33-t32)/1e9d;

		val t34 = System.nanoTime
		for(i<-ww){
			patternfile.write(i.toString)
			patternfile.write("\n")
		}
		val t35 = System.nanoTime
		sum14=sum14+(t35-t34)/1e9d;

		val t36 = System.nanoTime
		val data0size=data0.count
		val data1size=data1.count

		data0.unpersist()
		data1.unpersist()
		val finalgindex=if(t.size==0) 0 else{
			val tt=t.map(row=>(row._1,row._2._2))
			val ttt=tt.map{var v=0.0;row=>(row._1,row._1*row._1,{v+=row._2*10;v})}
			val midgindex=if(ttt.filter(row=>(row._2<=row._3)).size==0) 0 else{
			val f=ttt.filter(row=>(row._2<=row._3)).maxBy(_._1)
			val gindex=if (f._1==ttt.size) floor(sqrt(f._3)) else f._1
			gindex
			}
			midgindex
		}
		val toprint=finalgindex//*sqrt((data0.count+data1.count)/datasize.toDouble)
		patternfile.write(toprint.toString)
		//patternfile.close()
		//finalgindex//*sqrt((data0.count+data1.count)/datasize.toDouble)
		
		//finalgindex*(data0size+data1size)+4099/30.0*424/30.0)/((data0size+data1size)+4099/30.0)
		val fjia=if((data0size+data1size)==0) 0
		else (finalgindex*(data0size+data1size)+wholevalue*wholepop)/((data0size+data1size)+wholepop)
		patternfile.write(fjia.toString)
		patternfile.close()
		val t37 = System.nanoTime
		sum15=sum15+(t37-t36)/1e9d;

		fjia
	}
	
	def preCon()={
		var bucket=new ListBuffer[Map[String,(String, String)]]()
		def randomPick()={
			
			val k=flist.toMap
			val feaList=scala.util.Random.shuffle(k.keys.toVector).take(numofFeature)
		
			def log2(x: Double) = scala.math.log(x)/scala.math.log(2)
	
			for(i<-1 to log2(numofFeature.toDouble).toInt){
				for(j<-1 to 10){
					val yubei=feaList.map(a=>{val attr=scala.util.Random.shuffle(k(a)).take(2);val b=Map(a->(attr(0),attr(1)));b})
					val geshu=scala.util.Random.shuffle(yubei).take(i)
					val temp=geshu.reduce((a,b)=>a++b)
					bucket+=temp
				}
			}
		}
	
		randomPick()
		var wholevalue=0.0
		var wholepop=0.0
		var numBuc=0

		for(tempMap<-bucket){
			val left_popsize_new=sizeQuery(tempMap,"left")
			val right_popsize_new=sizeQuery(tempMap,"right")
			val populations=left_popsize_new+right_popsize_new
			
			if(populations>1){
			numBuc=numBuc+1
			wholepop+=populations
			val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
			println(tempMap)
			println("value is: "+value)
			wholevalue+=value
                        }
		}
		println("num of Buc: "+numBuc)
		
/*
		if(wholevalue==0||wholepop==0){
    		bucket=new ListBuffer[Map[String,(String, String)]]()  
     		val k=flist.toMap
     		val feaList=scala.util.Random.shuffle(k.keys.toVector).take(numofFeature)
  	
    		for(i<-1 to 5){
        	       for(j<-1 to 30){
        	               val yubei=feaList.map(a=>{val attr=scala.util.Random.shuffle(k(a)).take(2);val b=Map(a->(attr(0),attr(1)));b})
        	               val geshu=scala.util.Random.shuffle(yubei).take(i)
        	               val temp=geshu.reduce((a,b)=>a++b)
        	               bucket+=temp
        	       }
        	}
        	var wholevalue=0.0
		var wholepop=0.0
                var numBucket=0        
	
			for(tempMap<-bucket){
			        val left_popsize_new=sizeQuery(tempMap,"left")
			        val right_popsize_new=sizeQuery(tempMap,"right")
			        val populations=left_popsize_new+right_popsize_new
			        wholepop+=populations
			        val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
			        println(tempMap)
			        println("value is: "+value)
			        wholevalue+=value
			}
		}
*/
		println("wholepop: "+wholepop+",wholevalue: "+wholevalue)
		setpreCon(wholepop/numBuc,wholevalue/numBuc)
	}		


	/**
	* @param start node of the search path
	* @return
	*/
	def DFS(u:Node):Unit = {
		val s = new scala.collection.mutable.Stack[Node]
        var Listofnum= new ListBuffer[scala.collection.mutable.Map[String,(String, String)]]()
        s.push(u)
        while (!s.isEmpty)
        {
                val v=s.pop
                if (v.color==0){
                        v.color=2
                        countNode=countNode+1;
			if(!determineifin(Listofnum,v.treasureMap)) {
				val t6 = System.nanoTime
				insertyourchildren(v); 
				Listofnum+=v.treasureMap;
				val t7 = System.nanoTime
				sum1=sum1+(t7-t6)/1e9d
			}
			//	 val t6 = System.nanoTime
			//insertyourchildren(v);
			//val t7 = System.nanoTime
			//sum1=sum1+(t7-t6)/1e9d

			// if(v.children.size==0){
			// 					val t8 = System.nanoTime
   //                              output(v)
   //                              val t9 = System.nanoTime
   //                              sum2=sum2+(t9-t8)/1e9d
   //                      }else{
   //                              for(i<-0 to v.children.size-1){
   //                                      var w=v.children(i)
   //                                      w.pre=v
   //                                      if(w.value>=v.value){
   //                                              s.push(w)
   //                                      }else{
			// 									countNode=countNode+1;
			// 									val t10 = System.nanoTime
   //                                              output(w)
   //                                              val t11 = System.nanoTime
   //                                              sum2=sum2+(t11-t10)/1e9d
   //                                      }
   //                              }
   //                      }
                }
        }

		def determineifin(Listofnum:ListBuffer[scala.collection.mutable.Map[String,(String, String)]],newmap:scala.collection.mutable.Map[String,(String, String)]):Boolean = {
                def equaltomap(A:scala.collection.mutable.Map[String,(String, String)],B:scala.collection.mutable.Map[String,(String, String)]):Boolean = {
                        def equaltotuple(A:(String, String),B:(String, String)):Boolean={
                                if(A._1==B._1&&A._2==B._2) return true
                                else if(A._1==B._2&&A._2==B._1) return true
                                else false
                        }
                        if(A.keys!=B.keys) return false
                        else {
                                for(i<-A.keys){
                                        if(!equaltotuple(A(i),B(i))) return false
                                }
                        }
                        true
                }
                for(i<-Listofnum){
                        if(equaltomap(newmap,i)) return true
                }
                false
		}
	}

	/**
	* @param the node to be inserted
	* @return
	*/
	def insertyourchildren(u:Node) = {
		if(u.rem_fea.size!=0){

			var flag=false;
			val t12 = System.nanoTime;
			val l=if(u.pre==null) { flag=true; run(u); } else{ run2(u); }
			val t13 = System.nanoTime;
			if(flag) {
				sum3=sum3+(t13-t12)/1e9d;
			}else{
				sum4=sum4+(t13-t12)/1e9d;
			}

			for(i<-0 to l.size-1){
				val child=new Node(flist)
				child.sel_fea=l(i).select_fea
				child.rem_fea=l(i).remain_fea
				child.treasureMap=l(i).tempMap
				child.J=l(i).J_new
				child.k=l(i).k_new
				child.value=l(i).J_new(l(i).k_new);
				u.children=u.children:+child;
			}
		}
	}

	/**
	* @param the leaf node which cannot be expand
	* @return
	*/
	def output(u:Node):Unit = {
		var v=u;
		val pathfile = new FileWriter(FilePath+"path.txt", true) ;
		pathfile.write("path:"+u.treasureMap.mkString("  ")+"   "+u.value+"\n"); 
		while(v.pre!=null){
			pathfile.write("path:"+v.pre.treasureMap.mkString("  ")+"   "+v.pre.value+"\n"); 
			v=v.pre
		}
		println("\n\n");
		pathfile.write("finish path"+"\n"); 
		pathfile.close()
	}

	/**
	* @param tempMap: the temporal key and pairs, LorR: "left" or "right"
	* @return number of populations
	*/
	def sizeQuery(tempMap:Map[String,(String,String)],LorR:String):Long = {
		val query=if(LorR=="left") tempMap.map(s=>(s._1+"='"+s._2._1+"'")).mkString(" and ") 
				else tempMap.map(s=>(s._1+"='"+s._2._2+"'")).mkString(" and ")
		val result=sqlContext.sql(s"select * from testTable where $query")
		result.count
	}

	/**
	* @param u: node to be expand, here is the root node
	* @return list l of the children of node u
	*/
	def run(u:Node):List[pair] = {
        var treasureMap:Map[String,(String,String)]=Map.empty++=u.treasureMap
		var sel_fea=Set.empty++=u.sel_fea
		var rem_fea=Set.empty++=u.rem_fea
        //var beforecal=u.beforecal
        var J=u.J
        var k=u.k
        var l:List[pair]=List.empty
        
        for(fea <- feaSet){ 
        	val pairs=composePairs(4,flist.filter(_._1==fea)(0)._2)
        	for(p<-pairs){
        		val tempMap=treasureMap+(fea->p)
        		//println("tempMap is: "+tempMap)

        		val t40 = System.nanoTime
        		val left_popsize_new=sizeQuery(tempMap,"left")
				val right_popsize_new=sizeQuery(tempMap,"right")
				val t41 = System.nanoTime
				sum16=sum16+(t41-t40)/1e9d;

        		if(left_popsize_new>=80&&right_popsize_new>=80)
        		{
        			//calculate the population size to do bayesian average
        			val populations=left_popsize_new+right_popsize_new

        			val t16 = System.nanoTime
       				val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
        			val t17 = System.nanoTime
        			sum5=sum5+(t17-t16)/1e9d;
        			
        			val t42 = System.nanoTime
        			if(value!= -1){
        				val jiedian=new pair(value,fea,p,tempMap,sel_fea,rem_fea,J,k,populations)
       					l=jiedian::l
        			}
        			val t43 = System.nanoTime
        			sum17=sum17+(t43-t42)/1e9d;
        		}
        	}
        }
        for(i<-0 to l.size-1){
			val pathfile = new FileWriter(FilePath+"layer"+startfea+"-"+endfea+".txt", true) ;
			pathfile.write(wholevalue.toString+" "+wholepop.toString+" "+l(i).fea+" "+l(i).pair._1+" "+l(i).pair._2+" "+l(i).value+"\n")
			pathfile.close()
		}

        val t44 = System.nanoTime
        if(l.size!=0){
        	//val num=math.ceil(l.size*percentage).toInt

        	//total number of populations & avg score
        	// val pop=l.map(_.populations).sum*10
        	// val avg=l.map(_.value).sum/l.size
        	// val whole_score=pop*avg
        	// //calculate the true value after the bayesian
        	//  println("tempMap:"+treasureMap)

        	// println("pop: "+pop)
        	// println("avg: "+avg)

        	// l=l.map(a=>{a.value=(a.populations*a.value+whole_score)/(a.populations+pop);a})

        	//l=l.sortWith(_.value > _.value).take(num)

l=l.sortWith(_.value > _.value)
// val stan=l(0).value*(1-percentage)
// l=l.filter(a=>a.value>=stan)

/*calculate quatile value*/
val lsize=l.size
println("treasureMap size is: "+treasureMap.size)
println("select l, l min: "+l(lsize-1).value)
println("select l, l max: "+l(0).value)
println("lsize is: "+lsize)
if(lsize>=4){
if(lsize>=5){
println("select l, l first quatile: "+l((lsize/5).toInt-1).value)
}
println("select l, l first quatile: "+l((lsize/4).toInt-1).value)
println("select l, l second  quatile: "+l((lsize/2).toInt-1).value)
println("select l, l third quatile: "+l((3*lsize/4)-1).value)
}
//println("select l, l max: "+l(0).value)

val percentagefloat=l(0).value*(1-percentage)
val percentagesize=l.filter(_.value >= percentagefloat).size

var num=math.ceil(lsize*percentage).toInt
if(num==0) num=1

val finalnum=Math.max(percentagesize,num)
l=l.sortWith(_.value > _.value).take(finalnum)


//write to a file
// for(i<-0 to l.size-1){
// 	val pathfile = new FileWriter(FilePath+"firstselect.txt", true) ;
// 	pathfile.write(wholevalue.toString+" "+wholepop.toString+" "+l(i).fea+" "+l(i).pair._1+" "+l(i).pair._2+"\n")
// 	pathfile.close()
// }

//l=l.sortWith(_.value > _.value).take(num)
//l=l.sortWith(_.value > _.value).filter(_.value>=l(num-1).value)
}
val t45 = System.nanoTime
sum17=sum17+(t45-t44)/1e9d;
l
}
	/**
	* @param u: node to be expand
	* @return list l of the children of node u
	*/
	def run2(u:Node):List[pair] = {
		var treasureMap:Map[String,(String,String)]=Map.empty++=u.treasureMap
		var sel_fea=Set.empty++=u.sel_fea
		var rem_fea=Set.empty++=u.rem_fea
		//var beforecal=u.beforecal
		var J=u.J
		var k=u.k

		//Step1: select the best feature
		var l:List[pair]=List.empty
		var worst_fea:String=""
		var worst_pair:(String,String)=("","")
		
		for(fea <- rem_fea){
			val pairs=composePairs(3,flist.filter(_._1==fea)(0)._2)
			for(p<-pairs){
				val tempMap=treasureMap+(fea->p)
				//println("tempMap is: "+tempMap)
				val t46 = System.nanoTime
				val left_popsize_new=sizeQuery(tempMap,"left")
				val right_popsize_new=sizeQuery(tempMap,"right")
				val t47 = System.nanoTime
				sum18=sum18+(t47-t46)/1e9d;

				if(left_popsize_new>=80&&right_popsize_new>=80)
				{
					//calculate the population size to do bayesian average
        			val populations=left_popsize_new+right_popsize_new

        			val t18 = System.nanoTime
					val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
					val t19 = System.nanoTime
					sum6=sum6+(t19-t18)/1e9d;

					val t54 = System.nanoTime
					if(value!= -1){
						val jiedian=new pair(value,fea,p,tempMap,sel_fea,rem_fea,J,k-1,populations)
						l=jiedian::l
					}
					val t55 = System.nanoTime
					sum19=sum19+(t55-t54)/1e9d;
				}
			}
		}

		val t56 = System.nanoTime
					
		//first 2 elements in the list, means the top 2 values
		if(l.size!=0){
			//val num=math.ceil(l.size*percentage).toInt

			//total number of populations & avg score
        	// val pop=l.map(_.populations).sum*10
        	// val avg=l.map(_.value).sum/l.size
        	// val whole_score=pop*avg
        	// //calculate the true value after the bayesian
        	// println("tempMap:"+treasureMap)
        	// println("pop: "+pop)
        	// println("avg: "+avg)


        	// l=l.map(a=>{a.value=(a.populations*a.value+whole_score)/(a.populations+pop);a})

			//l=l.sortWith(_.value > _.value).take(num)

 		l=l.sortWith(_.value > _.value)
// val stan=l(0).value*(1-percentage)
// l=l.filter(a=>a.value>=stan)

/*calculate quatile value*/
val lsize=l.size
//val lsize=l.size
println("treasureMap size is: "+treasureMap.size)
println("select l, l min: "+l(lsize-1).value)
println("select l, l max: "+l(0).value)
println("lsize is: "+lsize)
if(lsize>=4){
println("select l, l first quatile: "+l((lsize/4).toInt-1).value)
println("select l, l second  quatile: "+l((lsize/2).toInt-1).value)
println("select l, l third quatile: "+l((3*lsize/4)-1).value)
}
val percentagefloat=l(0).value*(1-percentage)
val percentagesize=l.filter(_.value >= percentagefloat).size

var num=math.ceil(lsize*percentage).toInt
if(num==0) num=1

val finalnum=Math.max(percentagesize,num)
l=l.sortWith(_.value > _.value).take(finalnum)
//l=l.sortWith(_.value > _.value).filter(_.value>=l(num-1).value)
                }

                val t57 = System.nanoTime
				sum19=sum19+(t57-t56)/1e9d;
		
		for(i<-0 to l.size-1){
		//Step2: select the worst feature		

			case class worstnode(var value:Double,fea:String,populations:Double)
			
			var worst_templist:List[worstnode]=List.empty
			// var worst_fea:String=""
			// var worst_pair:(String,String)=("","")
	
			var cal_max= -1.0
			for(fea <- l(i).select_fea){
				val tempMap=l(i).tempMap-fea
				//println("tempMap is: "+tempMap)

				val t20 = System.nanoTime
				val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
				val t21 = System.nanoTime
				sum7=sum7+(t21-t20)/1e9d;

				if(value!= -1){

					val t48 = System.nanoTime
					

					val left_popsize_new=sizeQuery(tempMap,"left")
					val right_popsize_new=sizeQuery(tempMap,"right")

					val t49 = System.nanoTime
					sum18=sum18+(t49-t48)/1e9d;

					//calculate the population size to do bayesian average
        			val populations=left_popsize_new+right_popsize_new

        			val t58 = System.nanoTime
					
					val jiedian=worstnode(value,fea,populations)
					worst_templist=jiedian::worst_templist

					if(value>cal_max){
						cal_max=value;
						worst_fea=fea;
						worst_pair=l(i).tempMap(fea);
					}
					val t59 = System.nanoTime
					sum19=sum19+(t59-t58)/1e9d;
				}
			}

			// val pop=worst_templist.map(_.populations).sum*10
   //      	val avg=worst_templist.map(_.value).sum/worst_templist.size
   //      	val whole_score=pop*avg

   //      	println("tempMap:"+treasureMap)
   //      	println("pop: "+pop)
   //      	println("avg: "+avg)

   //      	//calculate the true value after the bayesian
   //      	worst_templist=worst_templist.map(a=>{a.value=(a.populations*a.value+whole_score)/(a.populations+pop);a})

			// worst_templist=worst_templist.sortWith(_.value > _.value).take(1)
			// if(worst_templist(0).value > cal_max){
			// 	cal_max=worst_templist(0).value;
			// 	worst_fea=worst_templist(0).fea;
			// 	worst_pair=l(i).tempMap(worst_templist(0).fea);
			// }


			//add according to book
			var beforecal:Double= -1.0
			/**
			*1) if the worst feature is the one which just added, you cannot drop it
			*2) if the value after dropping a feature is less or equal to the previous J value, you cannot drop it
			*3) if the k_new size is one, it means only one feature in the population feature, you cannot drop the feature to be added or the old one
			*Otherwise, add 1 to the k(index of J value) and update the J value because of adding a new feature
			*/
			if(worst_fea==l(i).fea||cal_max<=l(i).J_new(l(i).k_new)||l(i).k_new==1){
				val t60 = System.nanoTime
				
				l(i).k_new=l(i).k_new+1
				l(i).J_new(l(i).k_new)=l(i).value
				val t61 = System.nanoTime
				sum19=sum19+(t61-t60)/1e9d;
			}else{

				val t62 = System.nanoTime
					

				l(i).select_fea.remove(worst_fea);
				l(i).remain_fea.add(worst_fea);
				l(i).tempMap-=(worst_fea)
				beforecal=cal_max

				val t63 = System.nanoTime
					sum19=sum19+(t63-t62)/1e9d;

				if(l(i).k_new==2){
					l(i).J_new(l(i).k_new)=beforecal
				}else{
				//Step3: continue exclusion
					val t26 = System.nanoTime

					var worst_templist:List[worstnode]=List.empty
					cal_max= -1
					for(fea <- l(i).select_fea){
						val tempMap=l(i).tempMap-fea
						//println("tempMap is: "+tempMap)

						val t22 = System.nanoTime
						val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
						val t23 = System.nanoTime
						sum8=sum8+(t23-t22)/1e9d;

						if(value!= -1){

							val t50 = System.nanoTime
							
							val left_popsize_new=sizeQuery(tempMap,"left")
							val right_popsize_new=sizeQuery(tempMap,"right")
							val t51 = System.nanoTime
							sum18=sum18+(t51-t50)/1e9d;

							//calculate the population size to do bayesian average
        					val populations=left_popsize_new+right_popsize_new

        					val t64 = System.nanoTime

							val jiedian=worstnode(value,fea,populations)
							worst_templist=jiedian::worst_templist

							if(value>cal_max){
								cal_max=value;
								worst_fea=fea;
								worst_pair=l(i).tempMap(fea);
							}
							val t65 = System.nanoTime
							sum19=sum19+(t65-t64)/1e9d;
						}
					}
					
					//new add
					// val pop=worst_templist.map(_.populations).sum*10

					// val avg=worst_templist.map(_.value).sum/worst_templist.size
     //    			val whole_score=pop*avg

     //    			        	println("tempMap:"+treasureMap)
     //    	println("pop: "+pop)
     //    	println("avg: "+avg)

     //    			//calculate the true value after the bayesian
     //    			worst_templist=worst_templist.map(a=>{a.value=(a.populations*a.value+whole_score)/(a.populations+pop);a})

					// worst_templist=worst_templist.sortWith(_.value > _.value).take(1)
					// if(worst_templist(0).value >cal_max){
					// 	cal_max=worst_templist(0).value;
					// 	worst_fea=worst_templist(0).fea;
					// 	worst_pair=l(i).tempMap(worst_templist(0).fea);
					// }



					if(cal_max<l(i).J_new(l(i).k_new-1)){
						l(i).J_new(l(i).k_new)=beforecal
					}
					breakable{
						while(cal_max>=l(i).J_new(l(i).k_new-1)){
							val t66 = System.nanoTime
							
							beforecal=cal_max
							l(i).select_fea.remove(worst_fea);
							l(i).remain_fea.add(worst_fea);
							l(i).tempMap-=(worst_fea)
							l(i).k_new=l(i).k_new-1
							val t67 = System.nanoTime
							sum19=sum19+(t67-t66)/1e9d;

							if(l(i).k_new==2){
								l(i).J_new(l(i).k_new)=cal_max
								break
							}
							else{
								var worst_templist:List[worstnode]=List.empty
								cal_max= -1
								for(fea <- l(i).select_fea){
									val tempMap=l(i).tempMap-fea
									//println("tempMap is: "+tempMap)

									val t24 = System.nanoTime
									val value=conTrast(separateQuery(tempMap,"left"),separateQuery(tempMap,"right"),minSupport,tempMap.toString)
									val t25 = System.nanoTime
									sum9=sum9+(t25-t24)/1e9d;

									if(value!= -1){

										val t52 = System.nanoTime
										
										val left_popsize_new=sizeQuery(tempMap,"left")
										val right_popsize_new=sizeQuery(tempMap,"right")
										val t53 = System.nanoTime
										sum18=sum18+(t53-t52)/1e9d;
										//calculate the population size to do bayesian average
        								val populations=left_popsize_new+right_popsize_new
										
										val t68 = System.nanoTime
										
										val jiedian=worstnode(value,fea,populations)
										worst_templist=jiedian::worst_templist
										
										if(value>cal_max){
											cal_max=value;
											worst_fea=fea;
											worst_pair=l(i).tempMap(fea);
										}
										val t69 = System.nanoTime
										sum19=sum19+(t69-t68)/1e9d;
									}
								}

								// //new add
								// val pop=worst_templist.map(_.populations).sum*10
								// //new add
								// val avg=worst_templist.map(_.value).sum/worst_templist.size
        // 						val whole_score=pop*avg

        // 						        	println("tempMap:"+treasureMap)
        // 	println("pop: "+pop)
        // 	println("avg: "+avg)
        // 						//calculate the true value after the bayesian
        // 						worst_templist=worst_templist.map(a=>{a.value=(a.populations*a.value+whole_score)/(a.populations+pop);a})


								// worst_templist=worst_templist.sortWith(_.value > _.value).take(1)
								// if(worst_templist(0).value>cal_max){
								// 	cal_max=worst_templist(0).value;
								// 	worst_fea=worst_templist(0).fea;
								// 	worst_pair=l(i).tempMap(worst_templist(0).fea);
								// }




								if(cal_max<=l(i).J_new(l(i).k_new-1)) l(i).J_new(l(i).k_new)=beforecal
							}
						}
					}
					val t27 = System.nanoTime
					sum10=sum10+(t27-t26)/1e9d;
				}
			}
		}
		l
	}

	def rundata(data: RDD[String]){

		val t1 = System.nanoTime

		val t2 = System.nanoTime
		setfeaturelist(data)
		val t3 = System.nanoTime

		println(flist)
		println("successfully read the data!")

		//run 5 times and take the average

		val t4 = System.nanoTime
		// var pop:Double=0
		// var value:Double=0
		// for(i<- 1 to 5){
		// 	println("This is the ith times: "+i)
		// 	preCon()
		// 	pop=pop+wholepop
		// 	value=value+wholevalue
		// 	println("ith wholepop:"+wholepop+","+"ith wholevalue:"+wholevalue)
		// }
		// setpreCon(pop/5,value/5)
		// println("wholepop:"+wholepop+","+"wholevalue:"+wholevalue)
		val t5 = System.nanoTime

		

		val root=new Node(flist)
		DFS(root)

		val duration = (System.nanoTime - t1) / 1e9d
		println("Time duration is: "+duration)

		//record componet time
		val duration1=(t3-t2)/ 1e9d;
		val duration2=(t5-t4)/ 1e9d;
		println("Time duration1 is: "+duration1)
		println("Time duration2 is: "+duration2)
		println("Time duration3 is: "+sum1)
		println("Time duration4 is: "+sum2)
		println("Time duration5 is: "+sum3)
		println("Time duration6 is: "+sum4)
		println("Time duration7 is: "+sum5)
		println("Time duration8 is: "+sum6)
		println("Time duration9 is: "+sum7)
		println("Time duration10 is: "+sum8)
		println("Time duration11 is: "+sum9)
		println("Time duration12 is: "+sum10)
		println("Time duration13 is: "+sum11)
		println("Time duration14 is: "+sum12)
		println("Time duration15 is: "+sum13)
		println("Time duration16 is: "+sum14)
		println("Time duration17 is: "+sum15)
		println("Time duration18 is: "+sum16)
		println("Time duration19 is: "+sum17)
		println("Time duration20 is: "+sum18)
		println("Time duration21 is: "+sum19)


		println("all count Nodes is: "+countNode)
	}
}






