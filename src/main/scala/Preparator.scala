package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      items = trainingData.items,
      ratings = trainingData.ratings,
      likeEvents = trainingData.likeEvents)
  }
}

class PreparedData(
  val items: RDD[(Int, Item)],
  val ratings: RDD[Rating],
  val likeEvents: RDD[LikeEvent]
) extends Serializable
