package org.ardlema.enrichment

import java.util.{Collections, Properties}

import JavaSessionize.avro.{Client, Sale}
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsBuilder, StreamsConfig, Topology, TopologyTestDriver}
import org.ardlema.infra.{KafkaGlobalProperties, KafkaInfra}
import org.scalatest.{FunSpec, Matchers}

trait KafkaPropertiesEnrichment {

  val zookeeperHost = "localhost"
  val zookeeperPort = "2182"
  val zookeeperPortAsInt = zookeeperPort.toInt
  val kafkaHost = "localhost"
  val kafkaPort = "9093"
  val applicationKey = "enrichmentapp"
  val schemaRegistryHost = "localhost"
  val schemaRegistryPort = "8083"
}

class EnrichmentTopologySpec extends FunSpec with Matchers with KafkaGlobalProperties with KafkaPropertiesEnrichment with KafkaInfra {

  describe("The topology") {

    it("should enrichment the sales events") {
      val kafkaConfig = new Properties()
      kafkaConfig.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
      kafkaConfig.put("zookeeper.host", zookeeperHost)
      kafkaConfig.put("zookeeper.port", zookeeperPort)
      kafkaConfig.put(schemaRegistryUrlKey, s"""http://$schemaRegistryHost:$schemaRegistryPort""")
      kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
      kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, EnrichmentTopologyBuilder.getAvroSaleSerde().getClass.getName)
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
        val testDriver = new TopologyTestDriver(EnrichmentTopologyBuilder.createTopology(), kafkaConfig)
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), EnrichmentTopologyBuilder.getAvroSaleSerde().serializer())

        val sale1 = new Sale(1250.85F, "Jurassic Park T-shirt", 1234)
        val saleRecordFactory1 = recordFactory.create("input-topic-enrichment", "a", sale1)
        testDriver.pipeInput(saleRecordFactory1)
        val outputRecord1 = testDriver.readOutput("output-topic-enrichment", new StringDeserializer(), EnrichmentTopologyBuilder.getAvroSaleSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "a", sale1)

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


object EnrichmentTopologyBuilder extends KafkaPropertiesEnrichment {

  def getAvroSaleSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Sale]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

/*  def getAvroSaleAndStoreSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[SaleAndStore]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/"),
      false)
    specificAvroSerde
  }*/

  def createTopology(): Topology = {

    val builder = new StreamsBuilder()
    val initialStream = builder.stream("input-topic-enrichment", Consumed.`with`(Serdes.String(), getAvroSaleSerde())).to("output-topic-enrichment")
    builder.build()
  }

  //TODO: Make the proper transformations to the clientStream to get rid of the non VIP clients to make the test pass!!
  def filterVIPClients(clientStream: KStream[String, Client]): KStream[String, Client] = {
    clientStream
  }

}