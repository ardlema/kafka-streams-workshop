package org.ardlema.joining

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import JavaSessionize.avro.{Coupon, Purchase}
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams._
import org.ardlema.infra.{KafkaGlobalProperties, KafkaInfra}
import org.scalatest.FunSpec

trait KafkaPropertiesJoin {

  val zookeeperHost = "localhost"
  val zookeeperPort = "2183"
  val zookeeperPortAsInt = zookeeperPort.toInt
  val kafkaHost = "localhost"
  val kafkaPort = "9094"
  val applicationKey = "filtertapp"
  val schemaRegistryHost = "localhost"
  val schemaRegistryPort = "8083"
  val purchaseInputTopic = "purchase-input-topic"
  val couponInputTopic = "coupon-input-topic"
  val outputTopic = "joined-output-topic"
}

class JoinTopologySpec extends FunSpec with KafkaGlobalProperties with KafkaPropertiesJoin with KafkaInfra {

  describe("The topology") {

    it("should join sale events with the promo ones") {
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

        /*String str = "Jun 13 2003 23:11:52.454 UTC";
          SimpleDateFormat df = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");
          Date date = df.parse(str);
          long epoch = date.getTime();*/

        val coupon1 = new Coupon("Dec 05 2018 09:10:00.000 UTC", "1234", 10F)
        val purchase1 = new Purchase("Dec 05 2018 09:11:52.454 UTC", "1234", "Green Glass", 25.00F)
        val purchase1WithDiscount = new Purchase("Dec 05 2018 09:11:52.454 UTC", "1234", "Green Glass", 22.50F)
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

        /*val client2 = new Client("fran", 35, false)
        val consumerRecordFactory2 = recordFactory.create("input-topic", "b", client2, 9999L)
        testDriver.pipeInput(consumerRecordFactory2)
        val outputRecord2 = testDriver.readOutput("output-topic", new StringDeserializer(), FilterTopologyBuilder.getAvroSerde().deserializer())
        Assert.assertNull(outputRecord2)

        val client3 = new Client("maria", 37, true)
        val consumerRecordFactory3 = recordFactory.create("input-topic", "c", client3, 9999L)
        testDriver.pipeInput(consumerRecordFactory3)
        val outputRecord3 = testDriver.readOutput("output-topic", new StringDeserializer(), FilterTopologyBuilder.getAvroSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord3, "c", client3)*/
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


    val builder = new StreamsBuilder()
    val couponStream: KStream[String, Coupon] = builder.stream(couponInputTopic, Consumed.`with`(Serdes.String(), getAvroCouponSerde(schemaRegistryHost, schemaRegistryPort)))
    val purchaseStream = builder.stream(purchaseInputTopic, Consumed.`with`(Serdes.String(), getAvroPurchaseSerde(schemaRegistryHost, schemaRegistryPort)))

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

    val couponStreamKeyedByProductId: KStream[String, Coupon] = couponStream.selectKey(couponProductIdValueMapper)
    val purchaseStreamKeyedByProductId: KStream[String, Purchase] = purchaseStream.selectKey(purchaseProductIdValueMapper)

    val couponPurchaseValueJoiner = new ValueJoiner[Coupon, Purchase, Purchase]() {

      @Override
      def apply(coupon: Coupon, purchase: Purchase): Purchase = {
        if (coupon.getProductid.equals(purchase.getProductid))
          //TODO: APPLY THE DISCOUNT!!!!!!!!!
          new Purchase(purchase.getTimestamp, purchase.getProductid, purchase.getProductdescription, 22.50F)
        else
          new Purchase(purchase.getTimestamp, purchase.getProductid, purchase.getProductdescription, purchase.getAmount)
      }
    }

    val outputStream: KStream[String, Purchase] = couponStreamKeyedByProductId.join(purchaseStreamKeyedByProductId,
      couponPurchaseValueJoiner,
      JoinWindows.of(TimeUnit.MINUTES.toMillis(5)))

    outputStream.to(outputTopic)

    builder.build()
  }
}
