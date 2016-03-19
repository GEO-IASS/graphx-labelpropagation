package com.huawei.infolab.graph

import org.apache.spark.rdd.RDD
  import org.apache.spark.graphx.VertexId
  import org.apache.spark.graphx.Graph

class VertexInitializer extends java.io.Serializable{
  private val random = new java.util.Random()

  def apply(g:Graph[LPVertex, Double], labels:Array[String]):RDD[(VertexId, LPVertex)] = {
    val verts = g.vertices.map({case(vid, vdata) =>
      vdata.injectedLabels = normalize(vdata.injectedLabels)
      if(vdata.isSeedNode){
        vdata.estimatedLabels = vdata.injectedLabels
      }else{
        for(label <- labels){
          vdata.estimatedLabels += (label -> random.nextDouble())
        }
      }
      (vid, vdata)
    })
    verts
  }

  //normalize the values
  def normalize(m:Map[String,Double]):Map[String,Double] = {
    val sum = m.values.sum
    if(sum > 0){
      m.map(x => (x._1, x._2/sum))
    }else{
      m
    }
  }
}

