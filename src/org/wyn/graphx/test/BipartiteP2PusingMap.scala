package org.wyn.graphx.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.storage.StorageLevel
import org.apache.spark.graphx.VertexId
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import org.apache.log4j.Level


object BipartiteP2PusingMap {

 def main(args: Array[String]): Unit = {
   
 
  
   System.setProperty("hadoop.home.dir", "C:\\Hari\\env\\HadoopWinUtil")
     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)
    
     val conf = new SparkConf().setAppName("Bipartite Graph").setMaster("local")
     val sc = new SparkContext(conf)
     
     println("loading realation Persons -> ownes -> Phones ------------------------------>")
     // ownes relation b/w Perosn -> ownes -> Phone
     val mapPersonOwnedPhones = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/ownes_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            (cols(0).toLong , cols(2).toLong)
     }.collect().toMap
     
     mapPersonOwnedPhones.foreach(println)
     //   Phone (callar) ->  Calls -> Phone (calle) 
    // ownes relation b/w Perosn -> ownes -> Phone
     val edgesCalls: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/calls_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            Edge(mapPersonOwnedPhones.get(cols(1).toLong).get, mapPersonOwnedPhones.get(cols(2).toLong).get, cols(3))
                                    }
     
    val graphCalls: Graph[Int, String] = Graph.fromEdges(edgesCalls, defaultValue = 1,  StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK  ) 
   // val verticiesNew = nodes.map(node => (node.getVertexID, node))
    println("shortestpath b/w perosns -------------------------------------->")
    val callsShortestPath= AllPairShortestPath.runPairs(graphCalls)
    
    
    
    
    // final result
    println( "final result------------------->")  
    callsShortestPath.distinct.foreach(println)
  }

    
    
}