package org.ardlema.enrichment

import java.util.Collections

import JavaSessionize.avro.{Sale, SaleAndStore}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.{StreamsBuilder, Topology}

object EnrichmentTopologyBuilder {

  case class StoreInformation(storeAddress: String, storeCity: String)

  val storesInformation = Map(1234 -> StoreInformation("C/ Narvaez, 78", "Madrid"),
    5678 -> StoreInformation("C/ Pradillo, 33", "Madrid"))

  def getAvroSaleSerde(schemaRegistryHost: String, schemaRegistryPort: String) = {
    val specificAvroSerde = new SpecificAvroSerde[Sale]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def getAvroSaleAndStoreSerde(schemaRegistryHost: String, schemaRegistryPort: String) = {
    val specificAvroSerde = new SpecificAvroSerde[SaleAndStore]()
    specificAvroSerde.configure(
      Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, s"""http://$schemaRegistryHost:$schemaRegistryPort/"""),
      false)
    specificAvroSerde
  }

  def createTopology(schemaRegistryHost: String,
                     schemaRegistryPort: String,
                     inputTopic: String,
                     outputTopic: String,
                     outputTopicError: String): Topology = {


    val builder = new StreamsBuilder()
    val initialStream = builder.stream(inputTopic, Consumed.`with`(Serdes.String(), getAvroSaleSerde(schemaRegistryHost, schemaRegistryPort)))

    //TODO: Check out whether the store id from the sales event exists within the storesInformation hashmap. If it exists you should modify the event,
    // convert it to a SaleAndStore object and redirect it to the outputTopic topic. If it does not exist you should redirect the event to the outputTopicError topic.

    initialStream.to(outputTopic)
    initialStream.to(outputTopicError)

    builder.build()
  }
}
