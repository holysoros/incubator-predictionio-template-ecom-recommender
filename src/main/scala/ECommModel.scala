package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PersistentModel
import org.apache.predictionio.controller.PersistentModelLoader

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ECommModel(
    val rank: Int,
    val userFeatures: Map[Int, Array[Double]],
    val productModels: Map[Int, ProductModel])
  extends PersistentModel[ECommAlgorithmParams] {

  def save(id: String, params: ECommAlgorithmParams, sc: SparkContext): Boolean = {

    sc.parallelize(Seq(rank))
      .saveAsObjectFile(s"${params.modelSavePath}/${id}/rank")
    sc.parallelize(Seq(userFeatures))
      .saveAsObjectFile(s"${params.modelSavePath}/${id}/userFeatures")
    sc.parallelize(Seq(productModels))
      .saveAsObjectFile(s"${params.modelSavePath}/${id}/productModels")
    true
  }

  override def toString = {
    s"userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productModels: [${productModels.size}]" +
    s"(${productModels.take(2).toList}...)"
  }
}

object ECommModel extends PersistentModelLoader[ECommAlgorithmParams, ECommModel] {
  def apply(id: String, params: ECommAlgorithmParams, sc: Option[SparkContext]) = {
    val savePathPrefix = s"${params.modelSavePath}/${id}"
    val rankPath = s"${savePathPrefix}/rank"
    val userFeaturesPath = s"${savePathPrefix}/userFeatures"
    val productFeaturesPath = s"${savePathPrefix}/productFeatures"

    new ECommModel(
      rank = sc.get.objectFile[Int](rankPath).collect().head,
      userFeatures = sc.get.objectFile[Map[Int, Array[Double]]](userFeaturesPath).collect().head,
      productModels = sc.get.objectFile[Map[Int, ProductModel]](productFeaturesPath).collect().head
    )
  }
}
