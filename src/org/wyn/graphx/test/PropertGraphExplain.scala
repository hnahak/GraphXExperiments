package org.wyn.graphx.test

import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.log4j.Logger
import org.apache.log4j.Level

object PropertGraphExplain {
  
  
  def main(args: Array[String]): Unit = {
    
    System.setProperty("hadoop.home.dir", "C:\\Hari\\env\\HadoopWinUtil")
   Logger.getLogger("org").setLevel(Level.WARN)
   Logger.getLogger("akka").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("MyAPP").setMaster("local")
    val sc =  new SparkContext(conf)
   
   val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof")),
                       (4L, ("rxion", "postdoc")), (1L, ("rynold", "postdoc")),
                       (6L, ("shiva", "postdoc")), (8L, ("jhon", "student"))))
                       
    val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi"),
                       Edge(4L, 7L, "colleague"), Edge(1L, 7L, "colleague"),
                       Edge(6L, 7L, "colleague"), Edge(8L, 7L, "colleague")))
                       
    // Define a default user in case there are relationship with missing user
    val defaultUser = ("John Doe", "Missing")


    // Build the initial Graph
   val graph = Graph(users, relationships, defaultUser)
   
   graph.staticPageRank(5)
  // println(graph.vertices.count())
 
   // ########   Vertex Opertations ################ 
   
  // Count all users which are postdocs
  println ( graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.count)
  
  val postDocGraph = graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}
    
    postDocGraph
    
 // val postDocGraph = graph.subgraph(graph.triplets.filter{case ((name, edu),rel) => rel=="collegue" } )
   
 // Take the name   
 /*  println ( graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.first._2._1)       OR 

 graph.vertices.filter { case (id, (name, pos)) => pos == "postdoc"}.foreach(name => println(name._2._1))*/
   
   
   
   // ########## Edge Operations ##################
   
   // count 
   
  // println(graph.edges.filter { case Edge(src, dst, prop) => src > dst }.count)
  // Print each Edge ::::  
   //graph.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach(println)
 // Print each Edge Property  
   //graph.edges.filter { case Edge(src, dst, prop) => src > dst }.foreach( prop => println(prop.attr))
   
   
   
  }

}