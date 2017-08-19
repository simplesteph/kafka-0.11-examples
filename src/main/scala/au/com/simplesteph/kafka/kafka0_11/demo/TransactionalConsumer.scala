package au.com.simplesteph.kafka.kafka0_11.demo

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util

object TransactionalConsumer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("group.id", "my-transactional-consumer-group")
    props.put("enable.auto.commit", "false")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")


    props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED) // this has to be set!!!

    val consumer = new KafkaConsumer[_, _](props)
    consumer.subscribe(util.Arrays.asList("foo", "bar"))
    while (true) {
      val records = consumer.poll(100)
      for (record <- records) {
        ???
      }
      if (???) {
        // commit offsets once in a while
        consumer.commitSync()
      }
    }
  }
}
