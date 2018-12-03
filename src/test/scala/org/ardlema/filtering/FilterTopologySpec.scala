package org.ardlema.filtering

import org.ardlema.infra.KafkaInfra
import org.scalatest.{FunSpec, Matchers}

class FilterTopologySpec extends FunSpec with Matchers with KafkaInfra {

  describe("The topology") {

    /*it("should filter the VIP clients") {
      val kafkaConfig = new Properties()
      kafkaConfig.put(bootstrapServerKey, "localhost:9092")
      kafkaConfig.put("zookeeper.host", "localhost")
      kafkaConfig.put("zookeeper.port", "2181")
      kafkaConfig.put(schemaRegistryUrlKey, "http://localhost:8081")
      kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
      kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, FilterTopologyBuilder.getAvroSerde().getClass.getName)
      kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      kafkaConfig.put(groupIdKey, groupIdValue)
      kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
      kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
      kafkaConfig.put(applicationIdKey, "mystreamingapp")


      withKafkaServerAndSchemaRegistry(Option(kafkaConfig), true) { () =>
        val testDriver = new TopologyTestDriver(FilterTopologyBuilder.createTopology(), kafkaConfig)
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), FilterTopologyBuilder.getAvroSerde().serializer())

        val client1 = new Client("alberto", 39, true)
        val consumerRecordFactory1 = recordFactory.create("input-topic", "a", client1, 9999L)
        testDriver.pipeInput(consumerRecordFactory1)
        val outputRecord1 = testDriver.readOutput("output-topic", new StringDeserializer(), FilterTopologyBuilder.getAvroSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "a", client1)

        val client2 = new Client("fran", 35, false)
        val consumerRecordFactory2 = recordFactory.create("input-topic", "b", client2, 9999L)
        testDriver.pipeInput(consumerRecordFactory2)
        val outputRecord2 = testDriver.readOutput("output-topic", new StringDeserializer(), FilterTopologyBuilder.getAvroSerde().deserializer())
        Assert.assertNull(outputRecord2)

        val client3 = new Client("maria", 37, true)
        val consumerRecordFactory3 = recordFactory.create("input-topic", "c", client3, 9999L)
        testDriver.pipeInput(consumerRecordFactory3)
        val outputRecord3 = testDriver.readOutput("output-topic", new StringDeserializer(), FilterTopologyBuilder.getAvroSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord3, "c", client3)
      }
    }*/
  }
}