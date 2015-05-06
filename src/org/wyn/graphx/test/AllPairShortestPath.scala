package org.wyn.graphx.test

import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


object AllPairShortestPath {
  
 def runPairs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(VertexId, (VertexId, Int))] = {
    
   val g = graph
    // Remove redundant edges
    .groupEdges((a, b) => a)
    // Initialize vertices with the distance to themselves
    .mapVertices((id, attr) => Map(id -> 0))
    def unionMapsWithMin(a: Map[VertexId, Int], b: Map[VertexId, Int]): Map[VertexId, Int] = {
        (a.keySet ++ b.keySet).map { v =>
                val aDist = a.getOrElse(v, Int.MaxValue)
                val bDist = b.getOrElse(v, Int.MaxValue)
                v -> (if (aDist < bDist) aDist else bDist)
        }.toMap
    }
   
   // For sending update messages from a to b
    def diffMaps(a: Map[VertexId, Int], b: Map[VertexId, Int])
    : Map[VertexId, Int] = {
          (a.keySet ++ b.keySet).flatMap { v =>
          if (a.contains(v)) {
          val aDist = a(v)
          val bDist = b.getOrElse(v, Int.MaxValue)
          if (aDist + 1 < bDist) Some(v -> (aDist + 1)) else None
          } else {
          None
          }
        }.toMap
    }
   
   def sendMsg(et: EdgeTriplet[Map[VertexId, Int], ED])
        : Iterator[(VertexId, Map[VertexId, Int])] = {
            val msgToSrc = diffMaps(et.dstAttr, et.srcAttr)
            val msgToDst = diffMaps(et.srcAttr, et.dstAttr)
            Iterator((et.srcId, msgToSrc), (et.dstId, msgToDst)).filter(_._2.nonEmpty)
    }
   
   val maps = Pregel(g, Map.empty[VertexId, Int])(
    (id, a, b) => unionMapsWithMin(a, b), sendMsg, unionMapsWithMin)
    
    
   // println("pregel output----------------------------->")
   // maps.vertices.foreach(println)
    maps.vertices.flatMap { case (src, map) => map.iterator.map(kv => (src, kv)) }
  }
 
  def runVertexPairs[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED]): RDD[(VertexId, (VertexId, Int))] = {
    
   val g = graph
    // Remove redundant edges
    .groupEdges((a, b) => a)
    // Initialize vertices with the distance to themselves
    .mapVertices((id, attr) => (Map(id -> 0), attr))
    
   println("graph before pregel ----------------------------->")
    
   g.vertices.foreach(println)
    def unionMapsWithMin(a: Map[VertexId, Int], b: Map[VertexId, Int]): Map[VertexId, Int] = {
        (a.keySet ++ b.keySet).map { v =>
                val aDist = a.getOrElse(v, Int.MaxValue)
                val bDist = b.getOrElse(v, Int.MaxValue)
                v -> (if (aDist < bDist) aDist else bDist)
        }.toMap
    }
   
   
    def unionMapsWithMinNew(a: (Map[VertexId, Int] , VD) , b : (Map[VertexId, Int] , VD) ): (Map[VertexId, Int] , VD) = {
        
       val map = (a._1.keySet ++ b._1.keySet).map { v =>
                val aDist = a._1.getOrElse(v, Int.MaxValue)
                val bDist = b._1.getOrElse(v, Int.MaxValue)
                v -> (if (aDist < bDist) aDist else bDist)
        }.toMap
      
      return (map, a._2)
    }
  
   // For sending update messages from a to b
    def diffMaps(a: Map[VertexId, Int], b: Map[VertexId, Int])
    : Map[VertexId, Int] = {
          (a.keySet ++ b.keySet).flatMap { v =>
          if (a.contains(v)) {
          val aDist = a(v)
          val bDist = b.getOrElse(v, Int.MaxValue)
          if (aDist + 1 < bDist) Some(v -> (aDist + 1)) else None
          } else {
          None
          }
        }.toMap
    }
    
     def diffMapsNew(a: (Map[VertexId, Int] , VD)  , b : (Map[VertexId, Int] , VD) )
    : (Map[VertexId, Int] , VD)  = {
        val map =   (a._1.keySet ++ b._1.keySet).flatMap { v =>
          if (a._1.contains(v)) {
          val aDist = a._1(v)
          val bDist = b._1.getOrElse(v, Int.MaxValue)
          if (aDist + 1 < bDist) Some(v -> (aDist + 1)) else None
          } else {
          None
          }
        }.toMap
        
        return(map , a._2)
    }
     
   
   def sendMsg(et: EdgeTriplet[Map[VertexId, Int], ED])
        : Iterator[(VertexId, Map[VertexId, Int])] = {
            val msgToSrc = diffMaps(et.dstAttr, et.srcAttr)
            val msgToDst = diffMaps(et.srcAttr, et.dstAttr)
            Iterator((et.srcId, msgToSrc), (et.dstId, msgToDst)).filter(_._2.nonEmpty)
    }
   
   
   
   def sendMsgNew(et: EdgeTriplet[(Map[VertexId, Int] , VD), ED])
        : Iterator[(VertexId, (Map[VertexId, Int] , VD))] = {
            val msgToSrc = diffMapsNew(et.dstAttr, et.srcAttr)
            val msgToDst = diffMapsNew(et.srcAttr, et.dstAttr)
            Iterator((et.srcId, msgToSrc), (et.dstId, msgToDst)).filter(_._2._1.nonEmpty)
    }
   
   
   
   
  //  val maps = Pregel(g, Map.empty[VertexId, Int])(
  //  (id, a, b ) => unionMapsWithMin(a, b), sendMsg, unionMapsWithMin)
    
    
 /*   val maps = Pregel(g, (Map.empty[VertexId, Int]))(
    (id, a, b ) => unionMapsWithMinNew(a, b), sendMsgNew, unionMapsWithMinNew)
    
    
    println("pregel output----------------------------->")
    maps.vertices.foreach(println)
    maps.vertices.flatMap { case (src, map) => map.iterator.map(kv => (src, kv)) }*/
   
   return null
   
  }
 
  
  def runPairsUsingMap[VD: ClassTag, ED: ClassTag](graph: Graph[VD, ED], inputmap : Map[VertexId, Long]): Array[(Long, Long, Int)] = {
    
   val g = graph
    // Remove redundant edges
    .groupEdges((a, b) => a)
    // Initialize vertices with the distance to themselves
    .mapVertices((id, attr) => Map(id -> 0))
    def unionMapsWithMin(a: Map[VertexId, Int], b: Map[VertexId, Int]): Map[VertexId, Int] = {
        (a.keySet ++ b.keySet).map { v =>
                val aDist = a.getOrElse(v, Int.MaxValue)
                val bDist = b.getOrElse(v, Int.MaxValue)
                v -> (if (aDist < bDist) aDist else bDist)
        }.toMap
    }
   
   // For sending update messages from a to b
    def diffMaps(a: Map[VertexId, Int], b: Map[VertexId, Int])
    : Map[VertexId, Int] = {
          (a.keySet ++ b.keySet).flatMap { v =>
          if (a.contains(v)) {
          val aDist = a(v)
          val bDist = b.getOrElse(v, Int.MaxValue)
          if (aDist + 1 < bDist) Some(v -> (aDist + 1)) else None
          } else {
          None
          }
        }.toMap
    }
   
   def sendMsg(et: EdgeTriplet[Map[VertexId, Int], ED])
        : Iterator[(VertexId, Map[VertexId, Int])] = {
            val msgToSrc = diffMaps(et.dstAttr, et.srcAttr)
            val msgToDst = diffMaps(et.srcAttr, et.dstAttr)
            Iterator((et.srcId, msgToSrc), (et.dstId, msgToDst)).filter(_._2.nonEmpty)
    }
   
   val maps = Pregel(g, Map.empty[VertexId, Int])(
    (id, a, b) => unionMapsWithMin(a, b), sendMsg, unionMapsWithMin)
    
    
   // println("pregel output----------------------------->")
   // maps.vertices.foreach(println)
    maps.vertices.flatMap { case (src, map) => map.iterator.map(kv => (src, kv)) }
   
   val res = maps.vertices.flatMap { case (src, map) => map.iterator.map(kv => (src, kv._1, kv._2)) }.collect();
   
   println("pregel output----------------------------->")
   res.foreach(println)
   
   val finalres = res.map( pair => (inputmap.get(pair._1).get, inputmap.get(pair._2).get, pair._3))
   
   return finalres
   
  }
 
 }