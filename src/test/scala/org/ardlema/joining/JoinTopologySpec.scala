package org.ardlema.joining

import java.time.Duration
import java.util.Properties

import JavaSessionize.avro.{Coupon, Purchase}
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import kafka.server.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams._
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.ardlema.infra.{KafkaGlobalProperties, KafkaInfra}
import org.junit.Assert
import org.scalatest.{FlatSpec, Matchers}


trait KafkaPropertiesJsonConfigurer extends KafkaGlobalProperties {

  def getKafkaConfigProps(kafkaHost: String,
                          kafkaPort: String,
                          zookeeperHost: String,
                          zookeeperPort: String,
                          schemaRegistryHost: String,
                          schemaRegistryPort: String,
                          applicationKey: String,
                          logDir: String): Properties = {
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
    kafkaConfig.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, new GenericTimeStampExtractor().getClass.getName)
    kafkaConfig.put("log.dir", logDir)
    kafkaConfig
  }

  def getSchemaRegistryProps(kafkaHost: String, kafkaPort: String, schemaRegistryPort: String): Properties = {
    val schemaRegistryConfig = new Properties()
    schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, s"""PLAINTEXT://$kafkaHost:$kafkaPort""")
    schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "schemaregistrytopic")
    schemaRegistryConfig.put("port", schemaRegistryPort)

    schemaRegistryConfig
  }
}

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
  val logDir = "/tmp/kafka-join-logs"
}

trait KafkaPropertiesNotJoin {

  val zookeeperHost = "localhost"
  val zookeeperPort = "2185"
  val zookeeperPortAsInt = zookeeperPort.toInt
  val kafkaHost = "localhost"
  val kafkaPort = "9096"
  val applicationKey = "notjoinapp"
  val schemaRegistryHost = "localhost"
  val schemaRegistryPort = "8085"
  val purchaseInputTopic = "purchase-notjoin-input"
  val couponInputTopic = "coupon-notjoin-input"
  val outputTopic = "not-joined-output"
  val logDir = "/tmp/kafka-not-join-logs"
}

class JoinTopologySpec
  extends FlatSpec
    with KafkaPropertiesJoin
    with KafkaInfra
    with SystemClock
    with Matchers
    with KafkaPropertiesJsonConfigurer {

    "The topology" should "join sale events with the promo ones and apply the discounts" in new KafkaPropertiesJoin {
      val kafkaConfig = getKafkaConfigProps(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort, schemaRegistryHost, schemaRegistryPort, applicationKey, logDir)

      val schemaRegistryConfig = getSchemaRegistryProps(kafkaHost, kafkaPort, schemaRegistryPort)

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
        val coupon1TimePlusThreeMinutes = coupon1Time.plus(Duration.ofMinutes(3))
        val purchase1 = new Purchase(coupon1TimePlusThreeMinutes.toEpochMilli, "1234", "Red Glass", 25.00F)
        val couponRecordFactory1 = couponRecordFactory.create(couponInputTopic, "c1", coupon1)
        val purchaseRecordFactory1 = purchaseRecordFactory.create(purchaseInputTopic, "p1", purchase1)

        testDriver.pipeInput(couponRecordFactory1)
        testDriver.pipeInput(purchaseRecordFactory1)
        val outputRecord1: ProducerRecord[String, Purchase] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          JoinTopologyBuilder.getAvroPurchaseSerde(
            schemaRegistryHost,
            schemaRegistryPort).deserializer())
        outputRecord1.value().getAmount should be(22.50F)

        testDriver.close()
      }
    }

    "The topology" should "not join sale events when the purchase exceeds the timeout" in new KafkaPropertiesNotJoin {
      val kafkaConfig = getKafkaConfigProps(kafkaHost, kafkaPort, zookeeperHost, zookeeperPort, schemaRegistryHost, schemaRegistryPort, applicationKey, logDir)

      val schemaRegistryConfig = getSchemaRegistryProps(kafkaHost, kafkaPort, schemaRegistryPort)


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


        val coupon2Time = now()
        val coupon2 = new Coupon(coupon2Time.toEpochMilli, "5678", 10F)
        // Purchase after the five minutes of the coupon release - The discount should NOT be applied
        val coupon2TimePlusEightMinutes = coupon2Time.plus(Duration.ofMinutes(8))
        val purchase2 = new Purchase(coupon2TimePlusEightMinutes.toEpochMilli, "5678", "White Glass", 25.00F)
        val couponRecordFactory2 = couponRecordFactory.create(couponInputTopic, "c2", coupon2)
        val purchaseRecordFactory2 = purchaseRecordFactory.create(purchaseInputTopic, "p2", purchase2)
        testDriver.pipeInput(couponRecordFactory2)
        testDriver.pipeInput(purchaseRecordFactory2)
        val outputRecord2 = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          JoinTopologyBuilder.getAvroPurchaseSerde(
            schemaRegistryHost,
            schemaRegistryPort).deserializer())
        Assert.assertNull(outputRecord2)

        testDriver.close()
      }
    }
}




