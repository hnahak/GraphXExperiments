package org.wyn.graphx.test

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.bagel.Vertex
import org.apache.spark.rdd.RDD

object CSVConnector {

  def main(args: Array[String]): Unit = {
    
      val conf = new SparkConf().setAppName("GraphX Input from CSV Application").setMaster("local")
      val sc = new SparkContext(conf)
      val csvEntities = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/Phones_number_name_noheader.csv")
      
      val csvRelations = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecall_originator_recipient.csv")
      
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
                                                                            row => Edge(row(0).toLong, row(1).toLong, "1")
                                                                           )
                                                            )
                                                            
                                                            
     val edges: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecall_originator_recipient.csv")
                                     .map { line =>  val row = line.split(",")
                                            Edge(row(0).toLong, row(1).toLong, "1")
                                          } 
      
      
     val calls: Graph[Int, String] = Graph.fromEdges(edges, defaultValue = 1)                                                          
   
      
      
                                                            
    val graph = calls.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
                                                            
                                                            
  //     val defaultUser = ("John Doe", "Missing")
       
    // Build the initial Graph
//     val graph = Graph(users, relationships, defaultUser)
      
  //    Calcualting Page Rank 
 //    val pagerankGraph = graph.pageRank(0.001)
  //   println(pagerankGraph.vertices.top(5)(Ordering.by(_._2)).mkString("\n"))
     
  //    Vertex count 
    graph.vertices.take(5).foreach(println)
     
     /* Top 10 phone no. */
     
     
     
  }
}