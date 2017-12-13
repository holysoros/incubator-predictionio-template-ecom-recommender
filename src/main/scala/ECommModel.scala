package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PersistentModel
import org.apache.predictionio.controller.PersistentModelLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.twitter.chill.ScalaKryoInstantiator
import java.io.{FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}

class ECommModel(
    val userFeatures: RDD[(Int, Array[Double])],
    val productModels: RDD[(Int, ProductModel)])
  extends PersistentModel[ECommAlgorithmParams] {

  def save(id: String, params: ECommAlgorithmParams, sc: SparkContext): Boolean = {
    userFeatures.saveAsObjectFile(s"${params.modelSavePath}/${id}/userFeatures")
    productModels.saveAsObjectFile(s"${params.modelSavePath}/${id}/productModels")

    true
  }

  override def toString = {
    s"userFeatures: [${userFeatures.count()}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.count()}]" +
    s"(${productModels.take(2).toList}...)"
  }
}

object ECommModel extends PersistentModelLoader[ECommAlgorithmParams, ECommModel] {
  def apply(id: String, params: ECommAlgorithmParams, sc: Option[SparkContext]) = {
    val userFeatures: RDD[(Int, Array[Double])] = sc.get.objectFile(s"${params.modelSavePath}/${id}/userFeatures")
    val productModels: RDD[(Int, ProductModel)] = sc.get.objectFile(s"${params.modelSavePath}/${id}/productModels")
    new ECommModel(userFeatures, productModels)
  }
}
