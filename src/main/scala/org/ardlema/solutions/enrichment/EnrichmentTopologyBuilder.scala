package org.ardlema.solutions.enrichment

import java.util.Collections

import JavaSessionize.avro.{Sale, SaleAndStore}
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Predicate, ValueMapper}
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

    val existStore = new Predicate[String, Sale]() {
      @Override
      def test(key: String, sale: Sale): Boolean = {
        storesInformation.contains(sale.getStoreid)
      }
    }

    val notExistStore = new Predicate[String, Sale]() {
      @Override
      def test(key: String, sale: Sale): Boolean = {
        !storesInformation.contains(sale.getStoreid)
      }
    }

    val saleToSaleAndStore = new ValueMapper[Sale, SaleAndStore]() {
      @Override
      def apply(sale: Sale): SaleAndStore = {
        val storeInfo = storesInformation(sale.getStoreid)
        new SaleAndStore(sale.getAmount, sale.getProduct, storeInfo.storeAddress, storeInfo.storeCity)
      }
    }


    val splittedStream = initialStream.branch(existStore, notExistStore)

    splittedStream(0).mapValues[SaleAndStore](new ValueMapper[Sale, SaleAndStore]() {
      @Override
      def apply(sale: Sale): SaleAndStore = {
        val storeInfo = storesInformation(sale.getStoreid)
        new SaleAndStore(sale.getAmount, sale.getProduct, storeInfo.storeAddress, storeInfo.storeCity)
      }
    }).to(outputTopic)

    splittedStream(1).to(outputTopicError)
    builder.build()
  }
}
