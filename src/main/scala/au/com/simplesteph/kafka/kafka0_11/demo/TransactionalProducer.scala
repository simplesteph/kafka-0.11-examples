package au.com.simplesteph.kafka.kafka0_11.demo

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, Producer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.OutOfOrderSequenceException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.serialization.StringSerializer

object TransactionalProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id") // this has to be set!!! (unique for each producer you're having)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true") // has to be idempotent


    val producer = new KafkaProducer[String ,String](props, new StringSerializer, new StringSerializer)
    producer.initTransactions()

    try {
      producer.beginTransaction()
      for (i <- Range(0, 100)) {
        producer.send(new ProducerRecord[String, String]("my-transactional-topic", Integer.toString(i), Integer.toString(i)))
        producer.send(new ProducerRecord[String, String]("my-other-topic", Integer.toString(i), Integer.toString(i)))
      }
      producer.commitTransaction()
    } catch {
      case e@(_: ProducerFencedException | _: OutOfOrderSequenceException | _: AuthorizationException) =>
        // We can't recover from these exceptions, so our only option is to close the producer and exit.
        producer.close()
      case e: KafkaException =>
        // For all other exceptions, just abort the transaction and try again.
        producer.abortTransaction()
    }
  }
}
