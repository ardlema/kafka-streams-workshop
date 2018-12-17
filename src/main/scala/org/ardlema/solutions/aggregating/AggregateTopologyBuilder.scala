package org.ardlema.solutions.aggregating

import java.lang.{Long => JLong}
import java.util.Collections

import JavaSessionize.avro.Song
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder, Topology}
import org.apache.kafka.streams.kstream._

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

    val artistCount = new KeyValueMapper[String, Song, KeyValue[String, JLong]]() {

      @Override
      def apply(key: String, song: Song): KeyValue[String, JLong] = {
        new KeyValue[String, JLong](song.getArtist.toString, 1L)
      }
    }


    val artistCountStream: KStream[String, JLong] = initialStream.map(artistCount)
    val stringSerde: Serde[String] = Serdes.String()
    val longSerde: Serde[JLong] = Serdes.Long()
    val serialization = Serialized.`with`[String, JLong](stringSerde, longSerde)
    val songsGrouppedByArtist: KGroupedStream[String, JLong] = artistCountStream.groupByKey(serialization)
    val tableArtistAndCount = songsGrouppedByArtist.count()

    tableArtistAndCount.toStream.to(outputTopic, Produced.`with`(stringSerde, longSerde))
    builder.build()
  }
}
