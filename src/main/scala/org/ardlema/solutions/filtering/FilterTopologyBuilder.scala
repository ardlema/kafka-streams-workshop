package org.ardlema.solutions.filtering

import java.util.Collections

import JavaSessionize.avro.Client
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}

object FilterTopologyBuilder {

  def getAvroSerde() = {
    val specificAvroSerde = new SpecificAvroSerde[Client]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081/"),
      false)
    specificAvroSerde
  }

  def createTopology(): Topology = {
    implicit val keySerde: Serde[String] = Serdes.String
    implicit val valueSerde: SpecificAvroSerde[Client] = getAvroSerde()

    val builder = new StreamsBuilder()
    val initialStream: KStream[String, Client] = builder.stream("input-topic")
    val vipClientsStream = filterVIPClients(initialStream)
    vipClientsStream.to("output-topic")
    builder.build()
  }

  def filterVIPClients(clientStream: KStream[String, Client]): KStream[String, Client] = {
    clientStream.filter((_, client) => client.getVip)
  }
}
