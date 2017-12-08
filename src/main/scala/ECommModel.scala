package org.example.ecommercerecommendation

import org.apache.predictionio.controller.PersistentModel
import org.apache.predictionio.controller.PersistentModelLoader
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.twitter.chill.ScalaKryoInstantiator
import java.io.{FileInputStream, FileOutputStream}

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}

class ECommModel(
    val rank: Int,
    val userFeatures: Map[Int, Array[Double]],
    val productModels: Map[Int, ProductModel])
  extends PersistentModel[ECommAlgorithmParams] {

  def save(id: String, params: ECommAlgorithmParams, sc: SparkContext): Boolean = {
    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)

    val kryo = instantiator.newKryo()
    val fos = new FileOutputStream(s"${params.modelSavePath}/${id}")
    val output = new KryoOutput(fos, 4096)
    kryo.writeObject(output, this)
    output.close();
    fos.close();

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
    val instantiator = new ScalaKryoInstantiator
    instantiator.setRegistrationRequired(false)

    val kryo = instantiator.newKryo()
    val fis = new FileInputStream(s"${params.modelSavePath}/${id}")
    val input: KryoInput = new KryoInput(fis);
    val model: ECommModel = kryo.readObject(input, classOf[ECommModel]);
    input.close();

    model
  }
}
