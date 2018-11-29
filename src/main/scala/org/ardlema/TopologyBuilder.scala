package org.ardlema

import java.util.Collections

import JavaSessionize.avro.Client
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Predicate}
import org.apache.kafka.streams.{StreamsBuilder, Topology}

object TopologyBuilder {

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

    //KStream<String, Long> onlyPositives = stream.filter((key, value) -> value > 0);
    val isVipPredicate = new Predicate[String, Client]() {
      @Override
      def test(key: String, client: Client): Boolean = {
        client.getVip.booleanValue()
      }
    }
    val streamVIPs = initialStream.filter(isVipPredicate)

    streamVIPs.to("output-topic")
    builder.build()
  }

}