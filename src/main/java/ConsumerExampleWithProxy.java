import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.GenericRecordDomainDataDecoder;
import io.ebay.rheos.schema.event.RheosEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class ConsumerExampleWithProxy {
    public static void main(String[] args) {
        System.setProperty("java.security.auth.login.config", "/Users/lingtang/IdeaProjects/NuMessage-Portal/rkt/src/main/resources/kafka_jaas.conf");

        Properties props = new Properties();
        // This is the proxy addresses
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "rheos-kafka-proxy-1.phx02.dev.ebayc3.com:9093,rheos-kafka-proxy-2.phx02.dev.ebayc3.com:9093,rheos-kafka-proxy-3.phx02.dev.ebayc3.com:9093,rheos-kafka-proxy-1.lvs02.dev.ebayc3.com:9093,rheos-kafka-proxy-2.lvs02.dev.ebayc3.com:9093,rheos-kafka-proxy-3.lvs02.dev.ebayc3.com:9093");
        // This is the consumer name that is registered in Rheos
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "bes.bes1.caty.topic.bes.bes1.caty.item-new-slc");
        // Rheos uses byte[] as key
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        // Rheos event deserializer that decodes messages stored in Kafka to RheosEvent
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.ebay.rheos.schema.avro.RheosEventDeserializer");
        // This is client id registered in Rheos. This much match what is stored in Rheos or no data can be consumed
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "65639ee8-c6ab-475d-9d89-f4a788799176");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        /* Security properties
        See here https://github.corp.ebay.com/streaming-contrib/rheos-api/wiki/Rheos-0.0.4-SNAPSHOT-Auth&Auth#configure-iaf-in-kafka-client
        for how to import the required dependency.
        */
        props.put("sasl.mechanism", "IAF");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.login.class", "io.ebay.rheos.kafka.security.iaf.IAFLogin");
        props.put("sasl.callback.handler.class", "io.ebay.rheos.kafka.security.iaf.IAFCallbackHandler");

        /* below configs are up to users to define.
        Full config list can be found at http://kafka.apache.org/documentation.html#newconsumerconfigs
        */
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, 64 * 1024);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 16 * 1024);
        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RoundRobinAssignor.class.getName());

        final KafkaConsumer<byte[], RheosEvent> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("bes.bes1.caty.item-new"));

        // Setup the schema config
        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");
        GenericRecordDomainDataDecoder decoder = new GenericRecordDomainDataDecoder(config);

        while (true) {
            ConsumerRecords<byte[], RheosEvent> consumerRecords = kafkaConsumer.poll(Long.MAX_VALUE);
            Iterator<ConsumerRecord<byte[], RheosEvent>> iterator = consumerRecords.iterator();

            while (iterator.hasNext()) {
                ConsumerRecord<byte[], RheosEvent> record = iterator.next();
                GenericRecord payload = decoder.decode(record.value());
                System.out.println(payload);
            }
        }
    }
}