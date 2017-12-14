package org.example.ecommercerecommendation

import grizzled.slf4j.Logger
import org.apache.predictionio.controller.PersistentModel
import org.apache.predictionio.controller.PersistentModelLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class ECommModel(
    val userFeatures: RDD[(Int, Array[Double])],
    val productModels: RDD[(Int, ProductModel)])
  extends PersistentModel[ECommAlgorithmParams] {

  @transient lazy val logger: Logger = Logger[this.type]

  def save(id: String, params: ECommAlgorithmParams, sc: SparkContext): Boolean = {
    logger.info("Start to persist models")
    userFeatures.saveAsObjectFile(s"${params.modelSavePath}/${id}/userFeatures")
    productModels.saveAsObjectFile(s"${params.modelSavePath}/${id}/productModels")
    logger.info("Success to persist models")

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
  @transient lazy val logger: Logger = Logger[this.type]

  def apply(id: String, params: ECommAlgorithmParams, sc: Option[SparkContext]) = {
    logger.info("Start to load models")
    val userFeatures: RDD[(Int, Array[Double])] = sc.get.objectFile(s"${params.modelSavePath}/${id}/userFeatures")
    val productModels: RDD[(Int, ProductModel)] = sc.get.objectFile(s"${params.modelSavePath}/${id}/productModels")
    logger.info("Success to load models")
    logger.info(s"Size of userFeatures: ${userFeatures.count()}; size of productModels: ${productModels.count()}")
    new ECommModel(userFeatures, productModels)
  }
}
