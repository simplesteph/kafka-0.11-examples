package au.com.simplesteph.kafka.kafka0_11.demo

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.errors.{AuthorizationException, OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.StringSerializer


object ExactlyOnceLowLevel {



  def main(args: Array[String]): Unit = {

    val producer = createProducer()
    val consumer = createConsumer()
    producer.initTransactions()

    consumer.subscribe(util.Arrays.asList("foo", "bar"))

    try {

      while (true) {

        // read
        val records = consumer.poll(100)
        val producerRecords: List[ProducerRecord[String, String]] = doComplicatedStuff(records)
        producer.beginTransaction()
        producerRecords.foreach(producer.send)
        producer.sendOffsetsToTransaction(findOutOffsets(records), "my-transactional-consumer-group") // a bit annoying here to reference group id rwice
        producer.commitTransaction()
        // EXACTLY ONCE!
      }

    } catch {
      case e@(_: ProducerFencedException | _: OutOfOrderSequenceException | _: AuthorizationException) =>
        // We can't recover from these exceptions, so our only option is to close the producer and exit.
        producer.close()
      case e: KafkaException =>
        // For all other exceptions, just abort the transaction and try again.
        producer.abortTransaction()
    }


  }

  def createProducer(): KafkaProducer[String, String] = {
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id") // this has to be set!!! (unique for each producer you're having)
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // has to be idempotent
    new KafkaProducer[String, String](producerProps, new StringSerializer, new StringSerializer)
  }

  def createConsumer(): KafkaConsumer[String, String] = {
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("group.id", "my-transactional-consumer-group")
    consumerProps.put("enable.auto.commit", "false")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")

    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED) // this has to be set!!!
    new KafkaConsumer[String, String](consumerProps)
  }

  // map, filter, reduce, aggregate, have fun
  def doComplicatedStuff(records: ConsumerRecords[String, String]): List[ProducerRecord[String, String]] = ???

  // should be a helper method within kafka, but so far we have to implement it
  def findOutOffsets(records: ConsumerRecords[String, String]): util.Map[TopicPartition, OffsetAndMetadata] = ???


}
