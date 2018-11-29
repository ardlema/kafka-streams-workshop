package org.ardlema.infra

import java.util.{Collections, Properties}

import io.confluent.kafka.schemaregistry.rest.{SchemaRegistryConfig, SchemaRegistryRestApplication}
import kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer


trait KafkaInfra {

  val zookeeperPort = 2181
  val defaultSerializer = "org.apache.kafka.common.serialization.StringSerializer"
  val defaultDeserializer = "org.apache.kafka.common.serialization.StringDeserializer"
  val defaultAutoCreateTopics = "true"
  val defaultPartitions = "1"
  val defaultBrokerIdProp = "0"
  val zookeeperServer = "localhost"
  val bootstrapServerKey = "bootstrap.servers"
  val schemaRegistryUrlKey = "schema.registry.url"
  val keySerializerKey = "key.serializer"
  val keyDeserializerKey = "key.deserializer"
  val listenersKey = "listeners"
  val groupIdKey = "group.id"
  val groupIdValue = "prove_group"
  val valueSerializerKey = "value.serializer"
  val valueDeserializerKey = "value.deserializer"
  val kafkaHost = "localhost"
  val kafkaPort = "9092"
  val applicationIdKey = "application.id"
  val applicationIdValue = ""
  val autoCreateTopicsKey = "auto.create.topics.enable"
  val zookeeperPortConfig = "zookeeper.port"
  val zookeeperHostConfig = "zookeeper.host"
  val cacheMaxBytesBufferingKey = "cache.max.bytes.buffering"

  def configKafkaServer(props: Option[Properties], zkConnectString : String): Properties = {

    props match {
      case Some(properties) =>{
        properties.put(KafkaConfig.ZkConnectProp, zkConnectString)
        if(properties.getProperty(bootstrapServerKey) == null || properties.getProperty(bootstrapServerKey) == ""){
          properties.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
          properties.put(KafkaConfig.HostNameProp, kafkaHost)
          properties.put(KafkaConfig.PortProp, kafkaPort)
          properties.put(cacheMaxBytesBufferingKey, "0")
        }
        properties
      }
      case None =>{
        val kafkaConfig = new Properties()
        kafkaConfig.put(bootstrapServerKey, s"""$kafkaHost:$kafkaPort""")
        kafkaConfig.put(keySerializerKey, defaultSerializer)
        kafkaConfig.put(valueSerializerKey, defaultSerializer)
        kafkaConfig.put(keyDeserializerKey, defaultDeserializer)
        kafkaConfig.put(valueDeserializerKey, defaultDeserializer)
        kafkaConfig.put(groupIdKey, groupIdValue)
        kafkaConfig.put(KafkaConfig.BrokerIdProp, defaultBrokerIdProp)
        kafkaConfig.put(KafkaConfig.ZkConnectProp, zkConnectString)
        kafkaConfig.put(KafkaConfig.HostNameProp, kafkaHost)
        kafkaConfig.put(KafkaConfig.PortProp, kafkaPort)
        kafkaConfig.put(KafkaConfig.NumPartitionsProp, defaultPartitions)
        kafkaConfig.put(KafkaConfig.AutoCreateTopicsEnableProp, defaultAutoCreateTopics)
        kafkaConfig.put(cacheMaxBytesBufferingKey, "0")
        kafkaConfig
      }
    }
  }

  def withKafkaServer(props: Option[Properties], embedded: Boolean = false) (testFunction: KafkaServer => Any): Unit = {
    val conf = if(embedded) {
      val zookeeperServer = new TestingServer(zookeeperPort)
      zookeeperServer.start()
      configKafkaServer(props, zookeeperServer.getConnectString)
    }else {
      configKafkaServer(props, s"""${props.get.getProperty(zookeeperHostConfig)}:${props.get.getProperty(zookeeperPortConfig)}""")
    }

    val kafkaConfig = new KafkaConfig(conf)
    val kafkaServer = new KafkaServer(kafkaConfig)

    if(embedded){
      kafkaServer.startup()
    }
    testFunction(kafkaServer)
  }

  def withKafkaServerAndSchemaRegistry(props: Option[Properties], embedded: Boolean = false) (testFunction: () => Any): Unit = {
    val zookeeperServer = new TestingServer(zookeeperPort)
    val conf = if(embedded) {
      zookeeperServer.start()
      configKafkaServer(props, zookeeperServer.getConnectString)
    } else {
      configKafkaServer(props, s"""${props.get.getProperty(zookeeperHostConfig)}:${props.get.getProperty(zookeeperPortConfig)}""")
    }

    //TODO: Clean up this mess!!
    conf.put(schemaRegistryUrlKey, "http://localhost:8081")
    conf.put("offsets.topic.replication.factor", "1")
    val kafkaConfig = new KafkaConfig(conf)
    val kafkaServer = new KafkaServer(kafkaConfig)
    if(embedded){
      kafkaServer.startup()
      Thread.sleep(20000)
      val schemaRegistryConfig = new SchemaRegistryConfig(schemaRegistryProps(zookeeperServer.getConnectString))
      val restApp = new SchemaRegistryRestApplication(schemaRegistryConfig)
      val restServer = restApp.createServer()
      restServer.start()
    }
    testFunction()
  }

  def withKafkaProducer (props : Properties)(producerFunction: KafkaProducer[Any, Any] => Any): Unit = {
    val producer: KafkaProducer[Any, Any] = new KafkaProducer(props)
    producerFunction(producer)
  }

  def withKafkaConsumer (props : Properties, topics: List[String])(consumerFunction: KafkaConsumer[Any, Any] => Any): Unit = {
    val consumer: KafkaConsumer[Any, Any] = new KafkaConsumer(props)
    topics.foreach(topic => consumer.subscribe(Collections.singletonList(topic)))
    consumerFunction(consumer)
  }


  def fromKafkaConfigToProps(properties: KafkaConfig): Properties = {
    val props = new Properties()
    props.putAll(properties.props.asInstanceOf[java.util.Properties])
    props
  }

  private def schemaRegistryProps(zkConnect: String): Properties = {
    val schemaRegistryProps = new Properties()
    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, "PLAINTEXT://localhost:9092")
    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect)
    schemaRegistryProps.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, "schemaregistrytopic")
    schemaRegistryProps
  }
}
