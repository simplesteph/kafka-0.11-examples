package au.com.simplesteph.kafka.kafka0_11.demo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object IdempotentProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.RETRIES_CONFIG, 3) // this is now safe !!!!
    props.put(ProducerConfig.ACKS_CONFIG, "all") // this has to be all
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1) // this has to be 1

    val kafkaProducer = new KafkaProducer[String, String](props)

    // will send a producer record without duplicates as introduced by RETRIES
    kafkaProducer.send(new ProducerRecord[String, String]("my-topic-without-duplicates", "my-key", "my-value"))
    // this will still succeed, and a duplicate is in Kafka, because we wanted it!
    kafkaProducer.send(new ProducerRecord[String, String]("my-topic-without-duplicates", "my-key", "my-value"))

    kafkaProducer.close()

  }

}
