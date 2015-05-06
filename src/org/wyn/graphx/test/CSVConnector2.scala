package org.wyn.graphx.test

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.bagel.Vertex
import org.apache.spark.rdd.RDD



// Same as CSVConnector but it doesn't used method collect() for Edges making
object CSVConnector2 extends App {

  val conf = new SparkConf().setAppName("GraphX without collect() Input from CSV Application").setMaster("local")
     val sc = new SparkContext(conf)
                                                                        
     val users: RDD[(VertexId, (String, String))]  = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecalls/Phones_number_name_noheader.csv")
                                     .map { line =>  val row = line.split(",")
                                            (row(0).toLong, (row(1).toString(),"Phone"))
                                    }
  
     val edges: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecalls/phonecall_originator_recipient.csv")
                                     .map { line =>  val row = line.split(",")
                                            Edge(row(0).toLong, row(1).toLong, "1")
                                    } 
      
      
     val calls: Graph[Int, String] = Graph.fromEdges(edges, defaultValue = 1)                                                          
   
                                                                 
    val graph = calls.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
                                                            
                                                            
      
  //    Calcualting Page Rank 
     val pagerankGraph = graph.pageRank(0.001)
     println(pagerankGraph.vertices.top(5)(Ordering.by(_._2)).mkString("\n"))
     
  //    Vertex count 
   graph.vertices.take(5).foreach(println)
     
     /* Top 10 phone no. */
     
 
}