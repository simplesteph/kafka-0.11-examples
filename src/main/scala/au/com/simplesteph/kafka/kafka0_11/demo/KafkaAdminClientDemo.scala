package au.com.simplesteph.kafka.kafka0_11.demo

import java.util
import java.util.Properties

import org.apache.kafka.clients.admin._
import org.apache.kafka.common.{KafkaFuture, Node}
import org.apache.kafka.common.acl.{AccessControlEntry, AclBinding, AclOperation, AclPermissionType}
import org.apache.kafka.common.config.{ConfigResource, TopicConfig}
import org.apache.kafka.common.resource.{Resource, ResourceType}

import collection.JavaConverters._


object KafkaAdminClientDemo {
  def main(args: Array[String]): Unit = {

    val props = new Properties()
    props.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")

    val adminClient = AdminClient.create(props)

    // ACLs
    val newAcl = new AclBinding(new Resource(ResourceType.TOPIC, "my-secure-topic"), new AccessControlEntry("my-user", "*", AclOperation.WRITE, AclPermissionType.ALLOW))
    adminClient.createAcls(List(newAcl).asJavaCollection)
    // similarly
    adminClient.deleteAcls(???)
    adminClient.describeAcls(???)

    // TOPICS
    val numPartitions = 6
    val replicationFactor = 3.toShort
    val newTopic = new NewTopic("new-topic-name", numPartitions, replicationFactor)
    val configs = Map(TopicConfig.CLEANUP_POLICY_CONFIG -> TopicConfig.CLEANUP_POLICY_COMPACT,
      TopicConfig.COMPRESSION_TYPE_CONFIG -> "gzip")
    // settings some configs
    newTopic.configs(configs.asJava)
    adminClient.createTopics(List(newTopic).asJavaCollection)
    // similarly
    adminClient.deleteTopics(topicNames, options)
    adminClient.describeTopics(topicNames, options)
    adminClient.listTopics(new ListTopicsOptions().timeoutMs(500).listInternal(true))
    // describe topic configs
    adminClient.describeConfigs(List(new ConfigResource(ConfigResource.Type.TOPIC, TopicConfig.CLEANUP_POLICY_CONFIG)).asJavaCollection)
    adminClient.alterConfigs(???)


    // get Kafka configs -> make your topic respect your cluster defaults
    // get Kafka configs -> be crazy
    adminClient.describeConfigs(List(new ConfigResource(ConfigResource.Type.BROKER, "default.replication.factor")).asJavaCollection)
    adminClient.alterConfigs()


    // CLUSTER stuff
    val cluster = adminClient.describeCluster()
    val clusterId: KafkaFuture[String] = cluster.clusterId()
    val controller: KafkaFuture[Node] = cluster.controller()
    val nodes: KafkaFuture[util.Collection[Node]] = cluster.nodes()
    // nodes info
    Node.noNode().host()
    Node.noNode().id()
    Node.noNode().port()
    Node.noNode().rack()



  }
}
