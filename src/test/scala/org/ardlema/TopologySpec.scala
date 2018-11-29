package org.ardlema

import java.util.{Collections, Properties}

import JavaSessionize.avro.Client
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.{Consumed, Predicate}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology, TopologyTestDriver}
import org.ardlema.infra.KafkaInfra
import org.junit.Assert
import org.scalatest.{FunSpec, Matchers}

class TopologySpec extends FunSpec with Matchers with KafkaInfra {

  describe("The topology") {

    it("should filter the VIP clients") {
      val kafkaConfig = new Properties()
      kafkaConfig.put(bootstrapServerKey, "localhost:9092")
      kafkaConfig.put("zookeeper.host", "localhost")
      kafkaConfig.put("zookeeper.port", "2181")
      kafkaConfig.put(schemaRegistryUrlKey, "http://localhost:8081")
      kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
      kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TopologyBuilder.getAvroSerde().getClass.getName)
      kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      kafkaConfig.put(groupIdKey, groupIdValue)
      kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
      kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
      kafkaConfig.put(applicationIdKey, "mystreamingapp")


      withKafkaServerAndSchemaRegistry(Option(kafkaConfig), true) { () =>
        val testDriver = new TopologyTestDriver(TopologyBuilder.createTopology(), kafkaConfig)
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), TopologyBuilder.getAvroSerde().serializer())

        val client1 = new Client("alberto", 39, true)
        val consumerRecordFactory1 = recordFactory.create("input-topic", "a", client1, 9999L)
        testDriver.pipeInput(consumerRecordFactory1)
        val outputRecord1 = testDriver.readOutput("output-topic", new StringDeserializer(), TopologyBuilder.getAvroSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "a", client1)

        val client2 = new Client("fran", 35, false)
        val consumerRecordFactory2 = recordFactory.create("input-topic", "b", client2, 9999L)
        testDriver.pipeInput(consumerRecordFactory2)
        val outputRecord2 = testDriver.readOutput("output-topic", new StringDeserializer(), TopologyBuilder.getAvroSerde().deserializer())
        Assert.assertNull(outputRecord2)

        val client3 = new Client("maria", 37, true)
        val consumerRecordFactory3 = recordFactory.create("input-topic", "c", client3, 9999L)
        testDriver.pipeInput(consumerRecordFactory3)
        val outputRecord3 = testDriver.readOutput("output-topic", new StringDeserializer(), TopologyBuilder.getAvroSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord3, "c", client3)
      }
    }
  }
}

object TopologyBuilder {

  def getAvroSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Client]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/"),
      false)
    specificAvroSerde
  }

  def createTopology(): Topology = {

    val builder = new StreamsBuilder()
    val initialStream = builder.stream("input-topic", Consumed.`with`(Serdes.String(), getAvroSerde()))

    //KStream<String, Long> onlyPositives = stream.filter((key, value) -> value > 0);
    val isVipPredicate = new Predicate[String, Client]() {
      @Override
      def test(key: String, client: Client): Boolean = {
        client.getVip.booleanValue()
      }
    }
    val streamVIPs = initialStream.filter(isVipPredicate)

    streamVIPs.to("output-topic")
    builder.build()
  }
}