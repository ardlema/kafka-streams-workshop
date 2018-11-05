package org.ardlema

import java.lang
import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroSerializer
import kafka.server.KafkaConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.test.{ConsumerRecordFactory, OutputVerifier}
import org.apache.kafka.streams.{StreamsBuilder, Topology, TopologyTestDriver}
import org.ardlema.infra.KafkaInfra
import org.scalatest.{FunSpec, Matchers}

class TopologySpec extends FunSpec with Matchers with KafkaInfra {




  describe("The topology") {

    it("should send the elements to the output topic") {
      val kafkaConfig = new Properties()
      //TODO: Extract these properties to a dev environment config file!!
      //for now we ips need to be get from containers with inspect

      kafkaConfig.put(bootstrapServerKey, "localhost:9092")
      kafkaConfig.put("zookeeper.host", "localhost")
      kafkaConfig.put("zookeeper.port", "2181")
      kafkaConfig.put(schemaRegistryUrlKey, "http://localhost:8081")
      kafkaConfig.put(keyDeserializerKey, classOf[StringDeserializer])
      kafkaConfig.put(valueSerializerKey, classOf[KafkaAvroSerializer])
      kafkaConfig.put(groupIdKey, groupIdValue)
      kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
      kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
      kafkaConfig.put(applicationIdKey, "mystreamingapp")


      withKafkaServerAndSchemaRegistry(Option(kafkaConfig), true) { kafkaServer =>

        val testDriver = new TopologyTestDriver(TopologyBuilder.createTopology(), kafkaConfig)
        //val consumerRecord = factory.create("key", new java.lang.Integer(42))
        val stringDeserializer = new StringDeserializer()
        val longDeserializer = new LongDeserializer()
        val recordFactory = new ConsumerRecordFactory(new StringSerializer(), new LongSerializer())
        testDriver.pipeInput(recordFactory.create("input-topic", "a", 1L, 9999L))
        val outputRecord: ProducerRecord[String, lang.Long] = testDriver.readOutput("output-topic", new StringDeserializer(), new LongDeserializer())
        OutputVerifier.compareKeyValue[String, lang.Long](outputRecord, "a", 1L);
      }
    }
  }
}

object TopologyBuilder {

  def createTopology(): Topology = {
    val builder = new StreamsBuilder()
    builder.stream("input-topic").to("output-topic")
    builder.build()
  }
}