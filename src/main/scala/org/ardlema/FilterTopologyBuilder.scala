package org.ardlema

import java.util.Collections

import JavaSessionize.avro.Client
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, KStream}
import org.apache.kafka.streams.{StreamsBuilder, Topology}

object FilterTopologyBuilder {

  def getAvroSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Client]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/"),
      false)
    specificAvroSerde
  }

  def createTopology(): Topology = {

    val builder = new StreamsBuilder()
    val initialStream = builder.stream("input-topic", Consumed.`with`(Serdes.String(), getAvroSerde()))
    val vipClients = filterVIPClients(initialStream)
    vipClients.to("output-topic")
    builder.build()
  }

  //TODO: Make the proper transformations to the clientStream to get rid of the non VIP clients to make the test pass!!
  def filterVIPClients(clientStream: KStream[String, Client]): KStream[String, Client] = {
    clientStream
  }

}
