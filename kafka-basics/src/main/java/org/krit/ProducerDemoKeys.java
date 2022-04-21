package org.krit;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private final static Logger log = LoggerFactory
            .getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("start");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {

            String topic = "topic1";
            String value = "test value " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<>(
                            topic,
                            key,
                            value
                    );
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Received new metadata/ \n " +
                                "topic: " + metadata.topic() + "\n" +
                                "key: " + record.key() + "\n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp()
                        );
                    } else {
                        log.error("error", exception);
                    }
                }
            });

        }


        producer.flush();

        producer.close();
    }
}
