package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.controller.EmptyEvaluationInfo
import org.apache.predictionio.controller.EmptyActualResult
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import org.joda.time.DateTime
import org.joda.time.DateTimeZone

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, Item)
    val itemsRDD: RDD[(Int, Item)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
      val item = try {
        // Assume categories is optional property of item.
        Item(categories = properties.getOpt[List[String]]("categories"))
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId.toInt, item)
    }.persist(StorageLevel.MEMORY_AND_DISK_SER)

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("view", "like")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("item")))
    )(sc)

    val viewEventsRDD: RDD[ViewEvent] = eventsRDD
      .filter { event => event.event == "view" }
      .map { event =>
        try {
          ViewEvent(
            user = event.entityId.toInt,
            item = event.targetEntityId.get.toInt,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ViewEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val likeEventsRDD: RDD[LikeEvent] = eventsRDD
      .filter { event => event.event == "like" }
      .map { event =>
        try {
          LikeEvent(
            user = event.entityId.toInt,
            item = event.targetEntityId.get.toInt,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to LikeEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    new TrainingData(
      items = itemsRDD,
      viewEvents = viewEventsRDD,
      likeEvents = likeEventsRDD
    )
  }
}

case class User()

case class Item(categories: Option[List[String]])

case class ViewEvent(user: Int, item: Int, t: Long)

case class LikeEvent(user: Int, item: Int, t: Long)

class TrainingData(
  val items: RDD[(Int, Item)],
  val viewEvents: RDD[ViewEvent],
  val likeEvents: RDD[LikeEvent]
) extends Serializable {
  override def toString = {
    s"items: [${items.count()} (${items.take(2).toList}...)]" +
    s"viewEvents: [${viewEvents.count()}] (${viewEvents.take(2).toList}...)" +
    s"likeEvents: [${likeEvents.count()}] (${likeEvents.take(2).toList}...)"
  }
}
