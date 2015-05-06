package org.wyn.graphx.test

import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel


object GraphVertexEdge {
  
 def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MyAPP").setMaster("local")
    val sc =  new SparkContext(conf)
    
    val users = (sc.textFile("C:/Hari/inputs/graphx_input/users.txt").map(line => line.split(",")).map( parts => (parts.head.toLong, parts.tail) ))
 
   
   // http://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.graphx.GraphLoader 
    val calls = GraphLoader.edgeListFile(sc,"C:/Hari/inputs/graphx_input/followers.txt").persist(StorageLevel.MEMORY_ONLY) 
   /* val graph = calls.outerJoinVertices(users) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }*/

   
    calls.edges.foreach(println)
    
    
  //  SubGraph   val subgraph = graph.subgraph()
    
/*   MAX In/Out degrees of GRAPH 
   val maxInDegree: (VertexId, Int)  = graph.inDegrees.reduce(max)
 val maxOutDegree: (VertexId, Int) = graph.outDegrees.reduce(max)
val maxDegrees: (VertexId, Int)   = graph.degrees.reduce(max)


println("MaxInDegree:: " + graph.inDegrees.toArray().foreach(println) )
println("MaxInDegree:: " + maxInDegree.toString())
println("MaxOutDegree:: " + maxInDegree.toString())
println("MaxDegree:: " + maxInDegree.toString())*/
/*  PAGE RANK    val pagerankGraph = subgraph.pageRank(0.001)

    val userInfoWithPageRank = subgraph.outerJoinVertices(pagerankGraph.vertices) {
      case (uid, attrList, Some(pr)) => (pr, attrList.toList)
      case (uid, attrList, None) => (0.0, attrList.toList)
    }

    println(userInfoWithPageRank.vertices.top(5)(Ordering.by(_._2._1)).mkString("\n"))
*/ }
 
 def max(a: (VertexId, Int), b: (VertexId, Int)): (VertexId, Int) = {
  if (a._2 > b._2) a else b
}
}