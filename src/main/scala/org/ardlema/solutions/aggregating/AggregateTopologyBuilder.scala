package org.ardlema.solutions.aggregating

import java.util.Collections

import JavaSessionize.avro.Song
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}


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


    implicit val stringSerde: Serde[String] = Serdes.String
    implicit val longSerde: Serde[Long] = Serdes.Long
    implicit val avroSongSerde: SpecificAvroSerde[Song] = getAvroSongSerde(schemaRegistryHost, schemaRegistryPort)


    val builder: StreamsBuilder = new StreamsBuilder()
    val initialStream: KStream[String, Song] = builder.stream(inputTopic)

    val songsMappedByArtistStream: KStream[String, Long] = initialStream.map((_, song) => (song.getArtist.toString, 1L))

    val songsGroupByArtistStream: KGroupedStream[String, Long] = songsMappedByArtistStream.groupByKey

    val songsByArtistStream: KTable[String, Long] = songsGroupByArtistStream.count()

    songsByArtistStream.toStream.to(outputTopic)

    builder.build()
  }
}
