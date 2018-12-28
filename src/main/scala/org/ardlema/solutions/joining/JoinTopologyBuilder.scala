package org.ardlema.solutions.joining

import java.util.Collections

import JavaSessionize.avro.{Coupon, Purchase}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

import scala.concurrent.duration._

object JoinTopologyBuilder {

  def getAvroPurchaseSerde(schemaRegistryHost: String, schemaRegistryPort: String) = {
    val specificAvroSerde = new SpecificAvroSerde[Purchase]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def getAvroCouponSerde(schemaRegistryHost: String, schemaRegistryPort: String) = {
    val specificAvroSerde = new SpecificAvroSerde[Coupon]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def createTopology(schemaRegistryHost: String,
                     schemaRegistryPort: String,
                     couponInputTopic: String,
                     purchaseInputTopic: String,
                     outputTopic: String): Topology = {

    implicit val stringSerde: Serde[String] = Serdes.String
    implicit val avroPurchaseSerde: SpecificAvroSerde[Purchase] = getAvroPurchaseSerde(schemaRegistryHost, schemaRegistryPort)
    implicit val avroCouponSerde: SpecificAvroSerde[Coupon] = getAvroCouponSerde(schemaRegistryHost, schemaRegistryPort)

    val couponPurchaseValueJoiner: (Coupon, Purchase) => Purchase = (coupon: Coupon, purchase: Purchase) => {
      val discount = coupon.getDiscount * purchase.getAmount / 100
      new Purchase(purchase.getTimestamp, purchase.getProductid, purchase.getProductdescription, purchase.getAmount - discount)
    }

    val fiveMinuteWindow: JoinWindows = JoinWindows.of(5.minutes.toMillis).after(5.minutes.toMillis)

    val builder = new StreamsBuilder()

    val initialCouponStream: KStream[String, Coupon] = builder.stream(couponInputTopic)
    val initialPurchaseStream: KStream[String, Purchase] = builder.stream(purchaseInputTopic)

    val couponStreamKeyedByProductId: KStream[String, Coupon] = initialCouponStream
      .selectKey((_, coupon) => coupon.getProductid.toString)
    val purchaseStreamKeyedByProductId: KStream[String, Purchase] = initialPurchaseStream
      .selectKey((_, purchase) => purchase.getProductid.toString)

    couponStreamKeyedByProductId
      .join(purchaseStreamKeyedByProductId)(couponPurchaseValueJoiner, fiveMinuteWindow)
      .to(outputTopic)

    builder.build()
  }
}
