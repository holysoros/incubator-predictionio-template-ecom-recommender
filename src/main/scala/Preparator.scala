package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      items = trainingData.items,
      viewEvents = trainingData.viewEvents,
      likeEvents = trainingData.likeEvents)
  }
}

class PreparedData(
  val users: RDD[(Int, User)],
  val items: RDD[(Int, Item)],
  val viewEvents: RDD[ViewEvent],
  val likeEvents: RDD[LikeEvent]
) extends Serializable
