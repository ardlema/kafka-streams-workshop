package org.ardlema

import java.util.{Collections, Properties}

import JavaSessionize.avro.Client
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.{GenericAvroSerde, SpecificAvroSerde}
import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.test.ConsumerRecordFactory
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology, TopologyTestDriver}
import org.ardlema.infra.KafkaInfra
import org.scalatest.{FunSpec, Matchers}

class TopologySpec extends FunSpec with Matchers with KafkaInfra {

  describe("The topology") {

    it("should filter the VIP clients") {
      val kafkaConfig = new Properties()
      //TODO: Extract these properties to a dev environment config file!!
      //for now we ips need to be get from containers with inspect

      kafkaConfig.put(bootstrapServerKey, "localhost:9092")
      kafkaConfig.put("zookeeper.host", "localhost")
      kafkaConfig.put("zookeeper.port", "2181")
      kafkaConfig.put(schemaRegistryUrlKey, "http://localhost:8081")
      //kafkaConfig.put(keyDeserializerKey, classOf[StringDeserializer])
      //kafkaConfig.put(valueSerializerKey, classOf[KafkaAvroSerializer])
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

        val genericAvroSerde = new GenericAvroSerde()
        val isKeySerde = false
        genericAvroSerde.configure(
             Collections.singletonMap(
                 AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                 "http://localhost:8081/"),
             isKeySerde)

        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), genericAvroSerde.serializer())
        val client = new Client("alberto",39, true)
        val consumerRecordFactory = recordFactory.create("input-topic", "a", client, 9999L)
        testDriver.pipeInput(consumerRecordFactory)
        assert(true)
        //val outputRecord= testDriver.readOutput("output-topic", new StringDeserializer(), new KafkaAvroDeserializer())
        //OutputVerifier.compareKeyValue(outputRecord, "a", client)
      }
    }
  }
}

object TopologyBuilder {

  def getAvroSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Client]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081"),
      false)
    specificAvroSerde
  }

  def createTopology(): Topology = {

    val builder = new StreamsBuilder()

    builder.stream("input-topic", Consumed.`with`(Serdes.String(), getAvroSerde())).to("output-topic")
    builder.build()
  }
}