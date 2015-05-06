package org.wyn.graphx.test

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

object GraphFromVerteXEdgeArray {

  def main(args: Array[String]): Unit = {
    
      val conf = new SparkConf().setAppName("Graph using Vertex and Edge Array")
     val sc = new SparkContext(conf)
      
      
      val vertexArray = Array(
          (1L,("Alice",28)),
          (2L,("Bob",27)),
          (3L,("Charlie",65)),
          (4L,("David",42)),
          (5L,("Ed",55)),
          (6L,("Fran",50))
          )
      
      val edgeArray = Array(
          Edge(2L,1L,7),
          Edge(2L,4L,2),
          Edge(3L,2L,7),
          Edge(3L,6L,7),
          Edge(4L,1L,7),
          Edge(5L,2L,7),
          Edge(5L,3L,7),
          Edge(5L,6L,7),
          Edge(1L,2L,7))     
          
          
          val vertexRDD = sc.parallelize(vertexArray)
          val edgeRDD = sc.parallelize(edgeArray)
          
          val graph = Graph(vertexRDD, edgeRDD)
          
          val ageGT30 = graph.vertices.filter{
          case (id, (name, age)) => age > 30 
          }
          
          ageGT30.collect().foreach{
            case (id, (name, age) ) => println(s"$name is $age")
          }    
  }
}