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


trait Node {
  def getVertexID : Long  
}

class PersonNode(sID: String, sname : String) extends Node with Serializable {
  
  var id : Long = sID.toLong
  var name : String = sname
  
  override def getVertexID = id
  
  override def toString() = id + ":" + name
 }

class PhoneNode(sID: String, sNO : String) extends Node with Serializable{
  
  var id : Long = sID.toLong
  var number : Long = sNO.toLong
  
  override def getVertexID = id
  
  override def toString() = id + ":" + number
 }


object Bipartite {
  
  def main(args: Array[String]): Unit = {
    
    
     System.setProperty("hadoop.home.dir", "C:\\Hari\\env\\HadoopWinUtil")
     Logger.getLogger("org").setLevel(Level.WARN)
     Logger.getLogger("akka").setLevel(Level.WARN)
    
     val conf = new SparkConf().setAppName("Bipartite Graph").setMaster("local")
     val sc = new SparkContext(conf)
     
     println("loading Persons ------------------------------>")
     // PersonID, Name
     val persons: RDD[Node]  = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/persons_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                             ( new PersonNode(cols(0), cols(1)))
                                      }
     
     println("loading Phones ------------------------------>")
     // Phones ID, number
     val phones: RDD[Node]  = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/phones_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            ( new PhoneNode(cols(0), cols(1)))
                                    }
 
   //  println("No. of persons:" + persons.count())
   //  println("No. of phones :" + phones.count())
     
     // Join both RDDs 
     val nodes : RDD[Node] = persons.++(phones)
     nodes.persist()
     
    // println("Total Nodes :" + nodes.count()) 
  
     
     println("loading realation Persons -> ownes -> Phones ------------------------------>")
     // ownes relation b/w Perosn -> ownes -> Phone
     val edgesPersonOwnedPhones: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/ownes_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            Edge(cols(0).toLong, cols(2).toLong, "ownes")
                                    }
      
      
     val graphPersonOwnedPhones: Graph[Int, String] = Graph.fromEdges(edgesPersonOwnedPhones, defaultValue = 1, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK  )                                                         
   
     //println(" No of vertex from edgeList : " + graphPersonOwnedPhones.vertices.count())
   
     val verticiesNew = nodes.map(node => (node.getVertexID, node))
     
     
    
    val graph = graphPersonOwnedPhones.outerJoinVertices(verticiesNew) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
     
    
    println("vertex detials ------------------------------>")
    
    graph.vertices.foreach( vx => println(vx._1+" ----> "+vx._2))
      //println(newVertexCount.top(100)(Ordering.by(_._2)).mkString("\n"))
    
    println("edge b/w Phone and Person details --------------------------------->")
    
    graph.edges.foreach(println)
    
    println("loading relationship Calls--------------------------------------------------------------> ")
     
    //   Phone (callar) ->  Calls -> Phone (calle) 
    // ownes relation b/w Perosn -> ownes -> Phone
     val edgesCalls: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/calls_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            Edge(cols(1).toLong, cols(2).toLong, cols(3))
                                    }
      
    val graphCalls: Graph[Int, String] = Graph.fromEdges(edgesCalls, defaultValue = 1,  StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK  ) 
   // val verticiesNew = nodes.map(node => (node.getVertexID, node))
    println("edge b/w Phone and Person details --------------------------------->") 
    graphCalls.edges.foreach(println)
    
    
    
    
    
    println("shortestpath b/w calls -------------------------------------->")
    val callsShortestPath= AllPairShortestPath.runPairs(graphCalls)
     //val callsShortestPath= AllPairShortestPath.runVertexPairs(graphCalls, verticiesNew)
     
    // val res = callsShortestPath.join(verticiesNew)
    //callsShortestPath.filter( call => call._2._2 > 0).foreach(println)
    
    
     
     
    
     callsShortestPath.foreach(println)
     
     
    
  }
  
  

}