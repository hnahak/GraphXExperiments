package org.wyn.graphx.test

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}
import java.util.Calendar

object TestPageRankinSpark{
  
  def main(args: Array[String]): Unit = {
      /*if (args.length < 1) {
      System.err.println("Usage: SparkPageRank <file> <iter>")
      System.exit(1)
    }*/
    val sparkConf = new SparkConf().setAppName("PageRank").setMaster("local")
    
     println("Start Time :::"+ Calendar.getInstance().getTime())
    val t1 = System.currentTimeMillis
    
    
    val iters = if (args.length > 0) args(1).toInt else 1    // 1 is special case to handle phonecalls data which is generated randomly for 200mb/500mb files 
    val ctx = new SparkContext(sparkConf)
    val lines = ctx.textFile("C:\\Hari\\inputs\\graphx_input\\fromAmr\\new\\phonecall_1000rows.csv", 1)
    val links = lines.map{ s =>
      val parts = s.split("\\s+") //s.split(",")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()
    var ranks = links.mapValues(v => 1.0)

    
     
   // println("Value of links::: "+ lines.take(2).mkString("\n"))
    
    //println("Value of ranks::: "+ ranks.take(2).mkString("\n"))
    
    for (i <- 1 to iters) {
      val contribs = links.join(ranks).values.flatMap{ case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.collect()
    output.foreach(tup => println(tup._1 + " has rank: " + tup._2 + "."))
    
     println("End Time :::"+ Calendar.getInstance().getTime())
    val t2 = System.currentTimeMillis
    println((t2 - t1) + " msecs")

    ctx.stop()
  }

}