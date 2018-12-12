package org.ardlema.joining

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import JavaSessionize.avro.{Coupon, Purchase}
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import kafka.server.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.ardlema.infra.{KafkaGlobalProperties, KafkaInfra}
import org.ardlema.solutions.joining.{GenericTimeStampExtractor, SystemClock}
import org.scalatest.FunSpec

trait KafkaPropertiesJoin {

  val zookeeperHost = "localhost"
  val zookeeperPort = "2183"
  val zookeeperPortAsInt = zookeeperPort.toInt
  val kafkaHost = "localhost"
  val kafkaPort = "9094"
  val applicationKey = "joinapp"
  val schemaRegistryHost = "localhost"
  val schemaRegistryPort = "8083"
  val purchaseInputTopic = "purchase-input"
  val couponInputTopic = "coupon-input"
  val outputTopic = "joined-output"
}

class JoinTopologySpec
  extends FunSpec
    with KafkaGlobalProperties
    with KafkaPropertiesJoin
    with KafkaInfra
    with SystemClock {

  describe("The topology") {

    it("should join sale events with the promo ones and apply the discounts") {
      val kafkaConfig = new Properties()
      kafkaConfig.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
      kafkaConfig.put("zookeeper.host", zookeeperHost)
      kafkaConfig.put("zookeeper.port", zookeeperPort)
      kafkaConfig.put(schemaRegistryUrlKey, s"""http://$schemaRegistryHost:$schemaRegistryPort""")
      kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
      kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JoinTopologyBuilder.getAvroPurchaseSerde(schemaRegistryHost, schemaRegistryPort).getClass.getName)
      kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort""")
      kafkaConfig.put(groupIdKey, groupIdValue)
      kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
      kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
      kafkaConfig.put(applicationIdKey, applicationKey)
      kafkaConfig.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(cacheMaxBytesBufferingKey, "0")
      kafkaConfig.put("offsets.topic.replication.factor", "1")
      kafkaConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
      kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      //kafkaConfig.put("message.timestamp.type", "LogAppendTime")
      kafkaConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, new GenericTimeStampExtractor().getClass.getName)

      val schemaRegistryConfig = new Properties()
      schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, s"""PLAINTEXT://$kafkaHost:$kafkaPort""")
      schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "schemaregistrytopic")
      schemaRegistryConfig.put("port", schemaRegistryPort)


      withKafkaServerAndSchemaRegistry(kafkaConfig, schemaRegistryConfig, zookeeperPortAsInt) { () =>
        val testDriver = new TopologyTestDriver(JoinTopologyBuilder.createTopology(schemaRegistryHost,
          schemaRegistryPort,
          couponInputTopic,
          purchaseInputTopic,
          outputTopic), kafkaConfig)
        val purchaseRecordFactory = new ConsumerRecordFactory(new StringSerializer(),
          JoinTopologyBuilder.getAvroPurchaseSerde(
            schemaRegistryHost,
            schemaRegistryPort).serializer())
        val couponRecordFactory = new ConsumerRecordFactory(new StringSerializer(),
          JoinTopologyBuilder.getAvroCouponSerde(
            schemaRegistryHost,
            schemaRegistryPort).serializer())


        val coupon1Time = now()
        val coupon1 = new Coupon(coupon1Time.toEpochMilli, "1234", 10F)
        // Purchase within the five minutes after the coupon - The discount should be applied
        val coupon1TimePlusTwoMinutes = coupon1Time.plus(Duration.ofMinutes(2))
        val purchase1 = new Purchase(coupon1TimePlusTwoMinutes.toEpochMilli, "1234", "Red Glass", 25.00F)
        val purchase1WithDiscount = new Purchase(coupon1TimePlusTwoMinutes.toEpochMilli, "1234", "Red Glass", 22.50F)
        val couponRecordFactory1 = couponRecordFactory.create(couponInputTopic, "c1", coupon1)
        val purchaseRecordFactory1 = purchaseRecordFactory.create(purchaseInputTopic, "p1", purchase1)

        testDriver.pipeInput(couponRecordFactory1)
        testDriver.pipeInput(purchaseRecordFactory1)
        val outputRecord1 = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          JoinTopologyBuilder.getAvroPurchaseSerde(
            schemaRegistryHost,
            schemaRegistryPort).deserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "1234", purchase1WithDiscount)


        /*val coupon2 = new Coupon("Dec 05 2018 09:20:00.000 UTC", "5678", 10F)
        val couponRecordFactory2 = couponRecordFactory.create(couponInputTopic, "c2", coupon2)
        testDriver.pipeInput(couponRecordFactory2)
        // We advance the clock 7 minutes
        // Purchase after five minutes - The discount should NOT be applied
        val purchase2 = new Purchase("Dec 07 2019 09:27:00.454 UTC", "5678", "Green Glass", 40.00F)
        val purchaseRecordFactory2 = purchaseRecordFactory.create(purchaseInputTopic, "p2", purchase2)
        testDriver.pipeInput(purchaseRecordFactory2)

        val outputRecord2 = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          JoinTopologyBuilder.getAvroPurchaseSerde(
            schemaRegistryHost,
            schemaRegistryPort).deserializer())
        Assert.assertNull(outputRecord2)*/
      }
    }
  }
}

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

    val fiveMinuteWindow = JoinWindows.of(TimeUnit.MINUTES.toMillis(10))
    val outputStream: KStream[String, Purchase] = couponStreamKeyedByProductId.join(purchaseStreamKeyedByProductId,
      couponPurchaseValueJoiner,
      fiveMinuteWindow
      )

    outputStream.to(outputTopic)

    builder.build()
  }
}


