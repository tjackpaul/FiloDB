package filodb.kafka

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerConfig, SourceConfig}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

class MergeableConsumerConfigSpec extends ConfigSpec {
  "MergeableConsumerConfig" must {
    "consumer test" in {
      val topic = "test"

      val settings = new KafkaSettings(ConfigFactory.parseString(
        s"""
           |include file("$FullTestPropsPath")
           |filo-topic-name=$topic
           |filo-record-converter="filodb.kafka.StringRecordConverter"
        """.stripMargin))

      val config = new SourceConfig(settings.BootstrapServers, settings.clientId, settings.kafkaConfig)
      val values = config.kafkaConfig

      values(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) should be (settings.BootstrapServers)
      values(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) should be (classOf[LongDeserializer].getName)
      values(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) should be (classOf[StringDeserializer].getName)

      val props = values.asProps
      props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG) should be (settings.BootstrapServers)
      props.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG) should be (classOf[LongDeserializer].getName)
      props.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG) should be (classOf[StringDeserializer].getName)
    }
  }
}