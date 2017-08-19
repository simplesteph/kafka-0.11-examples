package au.com.simplesteph.kafka.kafka0_11.demo

import java.util

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import collection.JavaConverters._

object HeadersProducerRecord {
  def main(args: Array[String]): Unit = {

    // partition is null so we let kafka decide it
    // unfortunately no API without partition parameter (downside of Java overloads)
    val partition: Int = null

    // header can contain lots of good stuff!
    // origin / destination
    // timestamps along the way
    // unique id for auditability
    // routing information!
    // go crazy!
    val header: Header = new RecordHeader("key", "valueAsBytes".getBytes())
    val headers: util.List[Header] = List(header).asJava
    val producerRecord = new ProducerRecord[String, String]("topic", partition, "key", "value", headers)

    // better to set interceptors to set headers (set headers post .send() call)
  }
}
