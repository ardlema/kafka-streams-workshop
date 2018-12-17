package org.ardlema.aggregating

import java.lang.{Long => JLong}
import java.util.Properties

import JavaSessionize.avro.Song
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import kafka.server.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._

import scala.util.Random
import org.apache.kafka.streams._
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.ardlema.infra.{KafkaGlobalProperties, KafkaInfra}
import org.scalatest.{BeforeAndAfter, FunSpec}


trait KafkaPropertiesAggregate {

  //The clean-up method does not work for us so we generate a new app key for each test to overcome this issue
  def generateRandomApplicationKey: String = {
    val randomSeed = Random.alphanumeric
    randomSeed.take(12).mkString
  }

  val zookeeperHost = "localhost"
  val zookeeperPort = "2186"
  val zookeeperPortAsInt = zookeeperPort.toInt
  val kafkaHost = "localhost"
  val kafkaPort = "9097"
  val applicationKey = generateRandomApplicationKey
  val schemaRegistryHost = "localhost"
  val schemaRegistryPort = "8086"
  val logDir = "/tmp/kafka-aggregate-logs"
  val inputTopic = "song-input-topic"
  val outputTopic = "song-output-topic"
}

trait KafkaConfiguration extends KafkaPropertiesAggregate with KafkaGlobalProperties {
  val kafkaConfig = new Properties()
  kafkaConfig.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
  kafkaConfig.put("zookeeper.host", zookeeperHost)
  kafkaConfig.put("zookeeper.port", zookeeperPort)
  kafkaConfig.put(schemaRegistryUrlKey, s"""http://$schemaRegistryHost:$schemaRegistryPort""")
  kafkaConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
  kafkaConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
    AggregateTopologyBuilder.getAvroSongSerde(schemaRegistryHost, schemaRegistryPort).getClass.getName)
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
  kafkaConfig.put("log.dir", logDir)
  kafkaConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000")
  kafkaConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
}

class AggregateTopologySpec
  extends FunSpec
    with KafkaInfra
    with BeforeAndAfter
    with KafkaConfiguration {

  val topology = AggregateTopologyBuilder.createTopology(
    schemaRegistryHost,
    schemaRegistryPort,
    inputTopic,
    outputTopic)
  val testDriver = new TopologyTestDriver(topology, kafkaConfig)

  after {
    testDriver.close()
  }

  describe("The topology") {

    it("should aggregate the songs by artists") {
      val schemaRegistryConfig = new Properties()
      schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, s"""PLAINTEXT://$kafkaHost:$kafkaPort""")
      schemaRegistryConfig.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "schemaregistrytopic")
      schemaRegistryConfig.put("port", schemaRegistryPort)


      withKafkaServerAndSchemaRegistry(kafkaConfig, schemaRegistryConfig, zookeeperPortAsInt) { () =>
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(),
          AggregateTopologyBuilder.getAvroSongSerde(schemaRegistryHost, schemaRegistryPort).serializer())

        val song1 = new Song("Closer", 122, "Nine Inch Nails", "The Downward Spiral", "rock")
        val song2 = new Song("Heresy", 98, "Nine Inch Nails", "The Downward Spiral", "rock")
        val song3 = new Song("Wide Awake", 265, "Audioslave", "Revelations", "rock")
        val song4 = new Song("Wish", 112, "Nine Inch Nails", "Broken", "rock")
        val song5 = new Song("Until we fall", 215, "Audioslave", "Revelations", "rock")

        val consumerRecordFactory1 = recordFactory.create(inputTopic, "a", song1)
        val consumerRecordFactory2 = recordFactory.create(inputTopic, "a", song2)
        val consumerRecordFactory3 = recordFactory.create(inputTopic, "a", song3)
        val consumerRecordFactory4 = recordFactory.create(inputTopic, "a", song4)
        val consumerRecordFactory5 = recordFactory.create(inputTopic, "a", song5)
        testDriver.pipeInput(consumerRecordFactory1)
        val outputRecord1: ProducerRecord[String, JLong] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          new LongDeserializer())
        OutputVerifier.compareKeyValue(outputRecord1, "Nine Inch Nails", new JLong(1))
        testDriver.pipeInput(consumerRecordFactory2)
        val outputRecord2: ProducerRecord[String, JLong] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          new LongDeserializer())
        OutputVerifier.compareKeyValue(outputRecord2, "Nine Inch Nails", new JLong(2))
        testDriver.pipeInput(consumerRecordFactory3)
        val outputRecord3: ProducerRecord[String, JLong] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          new LongDeserializer())
        OutputVerifier.compareKeyValue(outputRecord3, "Audioslave", new JLong(1))
        testDriver.pipeInput(consumerRecordFactory4)
        val outputRecord4: ProducerRecord[String, JLong] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          new LongDeserializer())
        OutputVerifier.compareKeyValue(outputRecord4, "Nine Inch Nails", new JLong(3))
        testDriver.pipeInput(consumerRecordFactory5)
        val outputRecord5: ProducerRecord[String, JLong] = testDriver.readOutput(outputTopic,
          new StringDeserializer(),
          new LongDeserializer())
        OutputVerifier.compareKeyValue(outputRecord5, "Audioslave", new JLong(2))
      }
    }
  }
}
