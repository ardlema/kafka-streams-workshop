package org.ardlema.enrichment

import java.util.{Collections, Properties}

import JavaSessionize.avro.{Client, Sale, SaleAndStore}
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization.{Serdes, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Predicate, ValueMapper}
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
  val schemaRegistryPort = "8082"
  val inputTopic = "input-topic-enrichment"
  val outputTopic = "output-topic-enrichment-saleandstore"
  val outputTopicError = "output-topic-enrichment-error"
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
      schemaRegistryConfig.put("avro.compatibility.level", "none")


      withKafkaServerAndSchemaRegistry(kafkaConfig, schemaRegistryConfig, zookeeperPortAsInt) { () =>
        val testDriver = new TopologyTestDriver(EnrichmentTopologyBuilder.createTopology(), kafkaConfig)
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), EnrichmentTopologyBuilder.getAvroSaleSerde().serializer())

        val sale1 = new Sale(1250.85F, "Jurassic Park T-shirt", 1234)
        val saleAndStore1 = new SaleAndStore(1250.85F, "Jurassic Park T-shirt", "C/ Narvaez, 78", "Madrid")
        val saleRecordFactory1 = recordFactory.create(inputTopic, "a", sale1)
        testDriver.pipeInput(saleRecordFactory1)
        val outputRecord1 = testDriver.readOutput(outputTopic, new StringDeserializer(), EnrichmentTopologyBuilder.getAvroSaleAndStoreSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "a", saleAndStore1)

        val sale2 = new Sale(3434.85F, "Hat", 6666)
        val saleRecordFactory2 = recordFactory.create(inputTopic, "a", sale2)
        testDriver.pipeInput(saleRecordFactory2)
        val outputRecord2 = testDriver.readOutput(outputTopicError, new StringDeserializer(), EnrichmentTopologyBuilder.getAvroSaleSerde().deserializer())
        OutputVerifier.compareKeyValue(outputRecord2, "a", sale2)
      }
    }
  }
}


object EnrichmentTopologyBuilder extends KafkaPropertiesEnrichment {

  case class StoreInformation(storeAddress: String, storeCity: String)

  val storesInformation = Map(1234 -> StoreInformation("C/ Narvaez, 78", "Madrid"),
    5678 -> StoreInformation("C/ Pradillo, 33", "Madrid"))

  def getAvroSaleSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Sale]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def getAvroSaleAndStoreSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[SaleAndStore]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def createTopology(): Topology = {

    val builder = new StreamsBuilder()
    val initialStream = builder.stream(inputTopic, Consumed.`with`(Serdes.String(), getAvroSaleSerde()))

    val existStore = new Predicate[String, Sale]() {
      @Override
      def test(key: String, sale: Sale): Boolean = {
        storesInformation.contains(sale.getStoreid)
      }
    }

    val notExistStore = new Predicate[String, Sale]() {
      @Override
      def test(key: String, sale: Sale): Boolean = {
        !storesInformation.contains(sale.getStoreid)
      }
    }

    val saleToSaleAndStore = new ValueMapper[Sale, SaleAndStore]() {
      @Override
      def apply(sale: Sale): SaleAndStore = {
        val storeInfo = storesInformation(sale.getStoreid)
        new SaleAndStore(sale.getAmount, sale.getProduct, storeInfo.storeAddress, storeInfo.storeCity)
      }
    }


    val splittedStream = initialStream.branch(existStore, notExistStore)

    splittedStream(0).mapValues[SaleAndStore](new ValueMapper[Sale, SaleAndStore]() {
      @Override
      def apply(sale: Sale): SaleAndStore = {
        val storeInfo = storesInformation(sale.getStoreid)
        new SaleAndStore(sale.getAmount, sale.getProduct, storeInfo.storeAddress, storeInfo.storeCity)
      }
    }).to(outputTopic)

    splittedStream(1).to(outputTopicError)
    builder.build()
  }

  //TODO: Make the proper transformations to the clientStream to get rid of the non VIP clients to make the test pass!!
  def filterVIPClients(clientStream: KStream[String, Client]): KStream[String, Client] = {
    clientStream
  }

}