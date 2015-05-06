package org.wyn.graphx.test

    // Import random graph generation library 
import org.apache.spark.graphx.util.GraphGenerators 
import org.apache.spark.graphx._
import org.apache.spark._


// Took 58 mins in local ; 99 stages completed 
object ShortestPath {
  
  def main(args: Array[String]): Unit = {
    
     val conf = new SparkConf().setAppName("GraphX without collect() Input from CSV Application").setMaster("local")
     val sc = new SparkContext(conf)

// A graph with edge attributes containing distances 
val graph: Graph[Long, Int] = GraphGenerators.logNormalGraph(sc, numVertices = 100, numEParts = 100).mapEdges(e => e.attr.toInt) 
val sourceId: VertexId = 42 // The ultimate source 
// Initialize the graph such that all vertices except the root have distance infinity. 
val initialGraph = graph.mapVertices((id, _) => if (id == sourceId) 0.0 else Double.PositiveInfinity) 
val sssp = initialGraph.pregel(Double.PositiveInfinity)( 
  (id, dist, newDist) => math.min(dist, newDist), // Vertex Program 
  triplet => {  // Send Message 
    if (triplet.srcAttr + triplet.attr < triplet.dstAttr) { 
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr)) 
    } else { 
      Iterator.empty 
    } 
  }, 
  (a,b) => math.min(a,b) // Merge Message 
  ) 
println(sssp.vertices.collect.mkString("\n")) 
  }

  
  
}