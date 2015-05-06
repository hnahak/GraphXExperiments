package org.wyn.graphx.test.sql

   

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.bagel.Vertex
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext


/*// class to map phone call events data 
case class tblCallEvents(
                ONum: Long,
                RNum: Long,
                Timestamp: java.sql.Timestamp
                 
)

case class tblPhones(
      Num : Long,
      Name : String
)
  */


object SQLQuery  {
  
 
  def main(args: Array[String]): Unit = {
    
     var t1 = System.nanoTime() 
    
      val conf = new SparkConf().setAppName("GraphX Input from CSV Application").setMaster("local[4]").set("spark.executor.memory", "512m")
      val sc = new SparkContext(conf)
     
      val users: RDD[(VertexId, (String, String))]  = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/Phones_number_name_noheader.csv")
                                     .map { line =>  val row = line.split(",")
                                            (row(0).toLong, (row(1).toString(),"Phone"))
                                    }
     
     
      val edges: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/random_phonecalls0_200000.csv")
                                     .map { line =>  val row = line.split(",")
                                            Edge(row(1).toLong, row(2).toLong, row(3))
                                    } 
      
      
     val calls: Graph[(String, String), String] = Graph.fromEdges(edges, defaultValue = ("",""))                                                          
   
                                                                 
    val graph:Graph[(String, String), String] = calls.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => ("", "")
    }
     
     
     println("Count" + graph.edges.count()) 
     
     println("Count by value" + graph.edges.countByValue().take(2))
     
   /*  val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
     
     val sqlCntxt = new SQLContext(sc)
    
    import sqlCntxt._
    val cedRdd =   graph.edges.map(edge => new tblCallEvents(edge.srcId, edge.dstId, new java.sql.Timestamp(format.parse(edge.attr).getTime())))
     
     
    cedRdd.persist(StorageLevel.MEMORY_AND_DISK)
    cedRdd.registerTempTable("tblCallEvents")
      
    
     //Phones file name 
   //    val phonesFileName = "C:\\Hari\\inputs\\graphx_input\\fromAmr\\Phones_number_name_noheader - Org.csv"
    
    // read input file, drop first row and split data to table 
    val phonesRDD = sc.textFile(phonesFileName)
    .map(_.split(",", -1))
    .map(p => tblPhones(p(0).toLong, p(1)) )
    
    val phonesRDD = graph.vertices.map(vertex => tblPhones(vertex._1.toLong, vertex._2._1))
    
    phonesRDD.persist(StorageLevel.MEMORY_AND_DISK)
    
    phonesRDD.registerTempTable("tblPhones")
    
     //Top 100 Originators who have made most number of calls
    sql("SELECT Num, Name, Count(*) Calls FROM tblPhones as P, tblCallEvents E WHERE P.Num=E.ONum GROUP BY Num,Name ORDER BY Calls LIMIT 100").collect().foreach(println)
     
      var t2 = System.nanoTime() */
    
   // println("Time taken " + (t2-t1)/1000000000.0 + " Seconds")
  }
}
 