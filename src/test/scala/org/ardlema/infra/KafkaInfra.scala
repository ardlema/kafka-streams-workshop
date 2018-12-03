package org.ardlema.infra

import java.util.Properties

import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer


trait KafkaInfra {

  def withKafkaServerAndSchemaRegistry(kafkaProperties: Properties, schemaRegistryProps: Properties, zookeeperPort: Int) (testFunction: () => Any): Unit = {
    val zookeeperServer = new TestingServer(zookeeperPort)
    zookeeperServer.start()
    kafkaProperties.put(KafkaConfig.ZkConnectProp, zookeeperServer.getConnectString)
    val kafkaConfig: KafkaConfig = new KafkaConfig(kafkaProperties)
    val kafkaServer = new KafkaServer(kafkaConfig)
    kafkaServer.startup()
   schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zookeeperServer.getConnectString)
    val schemaRegistryConfig = new SchemaRegistryConfig(schemaRegistryProps)
    val restApp = new SchemaRegistryRestApplication(schemaRegistryConfig)
    val restServer = restApp.createServer()
    restServer.start()
    testFunction()
    kafkaServer.shutdown()
    zookeeperServer.stop()
  }
}
