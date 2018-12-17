package org.ardlema.aggregating

import java.lang.{Long => JLong}
import java.util.Collections

import JavaSessionize.avro.Song
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.{StreamsBuilder, Topology}

object AggregateTopologyBuilder {

  def getAvroSongSerde(schemaRegistryHost: String, schemaRegistryPort: String) = {
    val specificAvroSerde = new SpecificAvroSerde[Song]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def createTopology(schemaRegistryHost: String,
                     schemaRegistryPort: String,
                     inputTopic: String,
                     outputTopic: String): Topology = {

    val builder = new StreamsBuilder()
    val initialStream = builder.stream(inputTopic, Consumed.`with`(Serdes.String(), getAvroSongSerde(schemaRegistryHost, schemaRegistryPort)))

    //TODO: Add your code here to aggregate the songs to get the total count of songs by artist
    initialStream.to(outputTopic)
    builder.build()
  }
}
