package org.wyn.graphx.test.sql

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.bagel.Vertex
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext



// class to map phone call events data 
case class tblCallEvents(
                ONum: Long,
                RNum: Long,
                Timestamp: java.sql.Timestamp
                 
)

case class tblPhones(
      Num : Long,
      Name : String
)

object GraphXQuery {

  def main(args: Array[String]): Unit = {
    
     var t1 = System.nanoTime() 
    
      val conf = new SparkConf().setAppName("GraphX Input from CSV Application").setMaster("local[4]").set("spark.executor.memory", "512m")
      val sc = new SparkContext(conf)
      val csvEntities = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/Phones_number_name_noheader.csv")
      
      val csvRelations = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/random_phonecalls0_200000.csv")
      
      val users: RDD[(VertexId, (String, String))] = sc.parallelize(
                                                        csvEntities.map(
                                                                          line => ( line.split(","))
                                                                        )
                                                                    .collect
                                                                    .map(
                                                                          row => (row(0).toLong, (row(1).toString(),"Phone"))
                                                                         )
                                                                    )
      val relationships: RDD[Edge[String]] = sc.parallelize(
                                                      csvRelations.map(
                                                                          line => ( line.split(","))
                                                                      )
                                                                      .collect
                                                                      .map(
                                                                            row => Edge(row(1).toLong, row(2).toLong, row(3))
                                                                           )
                                                            )
      val defaultUser = ("John Doe", "Missing")
      val format = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
      // Build the initial Graph
     val graph = Graph(users, relationships, defaultUser)
      
     val sqlCntxt = new SQLContext(sc)
    
    import sqlCntxt._
    val cedRdd =   graph.edges.map(edge => new tblCallEvents(edge.srcId, edge.dstId, new java.sql.Timestamp(format.parse(edge.attr).getTime())))
     
     
     cedRdd.persist(StorageLevel.MEMORY_AND_DISK)
    cedRdd.registerTempTable("tblCallEvents")
      
    
     //Phones file name 
    val phonesFileName = "C:\\Hari\\inputs\\graphx_input\\fromAmr\\Phones_number_name_noheader - Org.csv"
    
    // read input file, drop first row and split data to table 
    /*val phonesRDD = sc.textFile(phonesFileName)
    .map(_.split(",", -1))
    .map(p => tblPhones(p(0).toLong, p(1)) )*/
    
    val phonesRDD = graph.vertices.map(vertex => tblPhones(vertex._1.toLong, vertex._2._1))
    
    phonesRDD.persist(StorageLevel.MEMORY_AND_DISK)
    
    phonesRDD.registerTempTable("tblPhones")
    
     //Top 100 Originators who have made most number of calls
    sql("SELECT Num, Name, Count(*) Calls FROM tblPhones as P, tblCallEvents E WHERE P.Num=E.ONum GROUP BY Num,Name ORDER BY Calls LIMIT 100").collect().foreach(println)
     
      var t2 = System.nanoTime() 
    
    println("Time taken " + (t2-t1)/1000000000.0 + " Seconds")
  }
}