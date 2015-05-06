package org.wyn.graphx.test

import scala.collection.mutable
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.bagel.Vertex
import org.apache.spark.rdd.RDD

object ReadingCSV  {

  def main(args: Array[String]): Unit = {
    
    
      val conf = new SparkConf().setAppName("GraphX Input from CSV Application").setMaster("local")
      val sc = new SparkContext(conf)
      val csvEntities = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/Phones_number_name_noheader.csv")
      
       val csvRelations = sc.textFile("C:/Hari/inputs/graphx_input/fromAmr/phonecall_originator_recipient.csv")
      
      //val data = csv.map(line => line.split(",").map(elem => elem.trim))
      
       
       //Making Vertex
      val vertex_rows = csvEntities.map(line => ( line.split(","))).collect()
      
      val vertexArray: Array[(Long, (String, String))]= vertex_rows.map( row => (row(0).toLong, (row(1).toString(),"Phone"))) 
      
      
      
      // Making edges 
       val edge_rows = csvRelations.map(line => ( line.split(","))).collect()
      
     // val edgeArray: Array[Edge[String]]= edge_rows.map( row => Edge(row(0).toLong, row(1).toLong, "1"))
      
     val edgeArray: Array[Edge[String]]= csvRelations.map(line => ( line.split(","))).collect().map( row => Edge(row(0).toLong, row(1).toLong, "1"))
      
     val users: RDD[(VertexId, (String, String))] = sc.parallelize(vertexArray)
       
     val relationships: RDD[Edge[String]] = sc.parallelize(edgeArray)
       
     val defaultUser = ("John Doe", "Missing")
       
         // Build the initial Graph
     val graph = Graph(users, relationships, defaultUser)
     
    
     
    
      
      /*val vertices : RDD[(VertexId, (String, String))] = sc.parallelize(seq, numSlices);
     for(row <- rows)
     {
       println("Originator:" + row(0) + " | Recepient:" + row(1))
     }
        */  
      
     // val firstCol = csv.map(line => line.split(",")(0))

     // firstCol.take(5).map(ele => println(ele)) 
      //firstCol.map(col => println(col))
      //println(firstCol.toArray().map(r => println(r)))
      
     // Print each columns from RDD ::: data.first().toArray.map(r => println(r))
      //val res = data.take(10).foreach( rdd => print(r
      
     
     /*  With Partition streaegy */
    
  }
}




/***
 * 
 * 
 * (3L, ("rxin", "student")
 * 
 * 
 */

