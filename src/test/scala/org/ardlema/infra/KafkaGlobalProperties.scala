package org.ardlema.infra

trait KafkaGlobalProperties {

  val defaultAutoCreateTopics = "true"
  val defaultPartitions = "1"
  val defaultBrokerIdProp = "0"
  val bootstrapServerKey = "bootstrap.servers"
  val schemaRegistryUrlKey = "schema.registry.url"
  val keySerializerKey = "key.serializer"
  val keyDeserializerKey = "key.deserializer"
  val listenersKey = "listeners"
  val groupIdKey = "group.id"
  val groupIdValue = "prove_group"
  val valueSerializerKey = "value.serializer"
  val valueDeserializerKey = "value.deserializer"
  val applicationIdKey = "application.id"
  val autoCreateTopicsKey = "auto.create.topics.enable"
  val zookeeperPortConfig = "zookeeper.port"
  val zookeeperHostConfig = "zookeeper.host"
  val cacheMaxBytesBufferingKey = "cache.max.bytes.buffering"
}
