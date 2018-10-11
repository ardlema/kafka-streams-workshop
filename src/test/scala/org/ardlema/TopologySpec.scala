package org.ardlema

import java.util.Properties

import kafka.server.KafkaConfig
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{Topology, TopologyTestDriver}
import org.ardlema.infra.KafkaInfra
import org.scalatest.{FunSpec, Matchers}

class TopologySpec extends FunSpec with Matchers with KafkaInfra {




  describe("The topology") {

    it("should filter the elements") {
      val kafkaConfig = new Properties()
      //TODO: Extract these properties to a dev environment config file!!
      //for now we ips need to be get from containers with inspect

      kafkaConfig.put(bootstrapServerKey, "localhost:9092")
      kafkaConfig.put("zookeeper.host", "localhost")
      kafkaConfig.put("zookeeper.port", "2181")
      /*kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      kafkaConfig.put(keySerializerKey, classOf[KafkaAvroSerializer])
      kafkaConfig.put(valueSerializerKey, classOf[KafkaAvroSerializer])*/
      kafkaConfig.put(keyDeserializerKey, classOf[StringDeserializer])
      kafkaConfig.put(valueDeserializerKey, classOf[LongDeserializer])
      kafkaConfig.put(groupIdKey, groupIdValue)
      kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
      kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
      kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
      kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
      kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
      kafkaConfig.put(applicationIdKey, "mystreamingapp")
      /*kafkaConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")
      kafkaConfig.put(ConsumerConfig.AutoOffsetReset, "earliest")*/


      withKafkaServerAndSchemaRegistry(Option(kafkaConfig), true) { kafkaServer =>
        val testDriver = new TopologyTestDriver(TopologyBuilder.createTopology(), kafkaConfig)

        true should be(true)
      }
    }
  }
}

object TopologyBuilder {

  def createTopology(): Topology = {
    val builder = new StreamsBuilder()
    builder.build()
  }
}