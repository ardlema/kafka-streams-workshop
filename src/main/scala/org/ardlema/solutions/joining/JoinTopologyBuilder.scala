package org.ardlema.solutions.joining

import java.util.Collections
import java.util.concurrent.TimeUnit

import JavaSessionize.avro.{Coupon, Purchase}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream._

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

    val couponProductIdValueMapper = new KeyValueMapper[String, Coupon, String]() {

      @Override
      def apply(key: String, value: Coupon): String = {
        value.getProductid.toString
      }
    }

    val purchaseProductIdValueMapper = new KeyValueMapper[String, Purchase, String]() {

      @Override
      def apply(key: String, value: Purchase): String = {
        value.getProductid.toString
      }
    }


    val builder = new StreamsBuilder()

    val couponConsumedWith = Consumed.`with`(Serdes.String(),
      getAvroCouponSerde(schemaRegistryHost, schemaRegistryPort))
    val couponStream: KStream[String, Coupon] = builder.stream(couponInputTopic, couponConsumedWith)

    val purchaseConsumedWith = Consumed.`with`(Serdes.String(),
      getAvroPurchaseSerde(schemaRegistryHost, schemaRegistryPort))
    val purchaseStream: KStream[String, Purchase] = builder.stream(purchaseInputTopic, purchaseConsumedWith)

    val couponStreamKeyedByProductId: KStream[String, Coupon] = couponStream.selectKey(couponProductIdValueMapper)
    val purchaseStreamKeyedByProductId: KStream[String, Purchase] = purchaseStream.selectKey(purchaseProductIdValueMapper)

    val couponPurchaseValueJoiner = new ValueJoiner[Coupon, Purchase, Purchase]() {

      @Override
      def apply(coupon: Coupon, purchase: Purchase): Purchase = {
        val discount = (purchase.getAmount * coupon.getDiscount) / 100
        new Purchase(purchase.getTimestamp, purchase.getProductid, purchase.getProductdescription, purchase.getAmount - discount)
      }
    }

    val fiveMinuteWindow = JoinWindows.of(TimeUnit.MINUTES.toMillis(5)).after(TimeUnit.MINUTES.toMillis(5))
    val outputStream: KStream[String, Purchase] = couponStreamKeyedByProductId.join(purchaseStreamKeyedByProductId,
      couponPurchaseValueJoiner,
      fiveMinuteWindow
    )

    outputStream.to(outputTopic)

    builder.build()
  }
}
