package org.wyn.graphx.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.storage.StorageLevel


class VertexProperty()
case class PersonProperty(val name: String ) extends VertexProperty
case class PhoneProperty(val name: String, val rel: String) extends VertexProperty



object BipartiteGraph {
  
  def main(args: Array[String]): Unit = {
    
    
     val conf = new SparkConf().setAppName("Bipartite Graph").setMaster("local")
     val sc = new SparkContext(conf) 
               
     // Person   ID, Name
     val persons: RDD[(VertexId, VertexProperty )]  = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/persons_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                             
                                            (cols(0).toLong, PersonProperty(cols(1)))
                                    }
     
   
     // Phones ID, Name(number)
     val phones: RDD[(VertexId, VertexProperty)]  = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/phones_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            (cols(0).toLong, PhoneProperty(cols(1) , cols(2)))
                                    }
     
     // Join both RDDs 
     val verticiesNew = persons.union(phones)
  
     // ownes relation b/w Perosn -> ownes -> Phone
     val edgesPersonOwnedPhones: RDD[Edge[String]] = sc.textFile("C:/Hari/inputs/graphx_input/bipartite/ownes_sample.txt")
                                     .map { row =>  val cols = row.split(",")
                                            Edge(cols(0).toLong, cols(2).toLong, "ownes")
                                    }
      
      
     val graphPersonOwnedPhones: Graph[Int, String] = Graph.fromEdges(edgesPersonOwnedPhones, defaultValue = 1, StorageLevel.MEMORY_AND_DISK, StorageLevel.MEMORY_AND_DISK  )                                                         
   
     println(graphPersonOwnedPhones.vertices.count())
   
    
    val graph = graphPersonOwnedPhones.outerJoinVertices(verticiesNew) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }
     
   /*  val graph2 = calls.outerJoinVertices(persons) {
      case (uid, deg, Some(attrList)) => attrList
      case (uid, deg, None) => Array.empty[String]
    }*/
    
     graph.vertices.foreach( vx => println(vx._2))
      //println(newVertexCount.top(100)(Ordering.by(_._2)).mkString("\n"))
  }

}