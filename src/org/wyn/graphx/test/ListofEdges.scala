package org.wyn.graphx.test

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object ListofEdges {
  
	def main(args: Array[String]): Unit = {
	  
	   val conf = new SparkConf().setAppName("GraphX without collect() Input from CSV Application").setMaster("local")
     val sc = new SparkContext(conf)
                                                                        
     val users: RDD[(VertexId, (String, String))]  = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecalls/Phones_number_name_noheader.csv")
                                     .map { line =>  val row = line.split(",")
                                            (row(0).toLong, (row(1).toString(),"Phone"))
                                    }
  
     val edges: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecalls/random_phonecalls0_200000.csv")
                                     .map { line =>  val row = line.split(",")
                                            Edge(row(1).toLong, row(2).toLong, "1")
                                    }
      
      
     val calls: Graph[Int, String] = Graph.fromEdges(edges, defaultValue = 1, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK  )                                                         
   
                                                                 
   /*   IF we are calculating pagerank then no need of joining verticies before, 
    *   because after you achinve PageRabk graph to know the vertices you will have to join once again
    *   
    */  
       val graph = calls.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
                                                            
                                                            
      
  //    Calcualting Page Rank 
  //  val pagerankGraph = graph.pageRank(0.001)
  //   println(pagerankGraph.vertices.top(5)(Ordering.by(_._2)).mkString("\n"))
     val vertexArray = graph.ops.collectEdges(EdgeDirection.In)
     
    
     
     //val newVertex=  vertexArray.map( vertex => (vertex._1, vertex._2))
     
     val newVertexCount=  vertexArray.map( vertex => (vertex._1, vertex._2.length))
     
     println(newVertexCount.top(5)(Ordering.by(_._2)).mkString("\n"))
      /*  Toatl In degeress of a verticies */     
     //     newVertex.foreach(row =>  println(row._1.toString() + ": Total Edges" + row._2.length))
     
     /* 7159204332: Total Edges1039
     
      3021396188: Total Edges901
      8145994006: Total Edges862
      2158406636: Total Edges862
      3092724207: Total Edges959
      7327200505: Total Edges1032
      * */ 
    
   // TO DO list of In Deg vertcies   newVertex.foreach(row =>  println(row._1.toString() + ":-> Dst  ID -->" + row._2.foreach(edge => println(edge.srcId.toString()))))
     
   //s  val ngb = graph.ops.
     
     

	}

}