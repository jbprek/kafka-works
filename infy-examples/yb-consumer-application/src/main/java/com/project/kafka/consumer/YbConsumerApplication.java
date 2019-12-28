
package com.project.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.StreamSupport;

@SpringBootApplication
@Slf4j
public class YbConsumerApplication implements CommandLineRunner {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static KafkaConsumer<String, String> streamConsumer;

    private static final String producerTopic = "ybtopic";
    private static final String filteredTopic = "ybTruant";



    /*
     * Configure & Create Kafka Producer
     */
    private static KafkaConsumer<String, String> createConsumer(String ... topics){
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "yb-group");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties);
        // Subscribe to topic
        consumer.subscribe(Arrays.asList((topics)));

        return consumer;
    }

    @Override
    public void run(String... args) {

        streamConsumer = createConsumer(producerTopic, filteredTopic);

        int recordCount = 0;
        streamConsumer.poll(durationOfMillis(0));
        streamConsumer.seekToBeginning(streamConsumer.assignment());

        while (true) {
            ConsumerRecords<String, String> records = streamConsumer.poll(durationOfMillis(3000));
            if (records.count() > 0) {
                recordCount += records.count();

                StreamSupport.stream(records.spliterator(), false).forEach(YbConsumerApplication::logRecordInfo);
                streamConsumer.commitAsync();
            }
        }

    }

    // TODO graceful shutdown check
    @PreDestroy
    public void tearDown() {
        log.info("Exiting application, closing Kafka session");
        streamConsumer.close();
    }

    private Duration durationOfMillis(long millis) {
        return Duration.of(millis, ChronoUnit.MILLIS);
    }

    public static void logRecordInfo(ConsumerRecord<String, String> record) {
        log.info("Consumer Record:Topic - {}  Key - {}  Value - {} Partition - {} Offset {}", record.topic(), record.key(), record.value(), record.partition(), record.offset());
    }

    public static void main(String[] args) {
        SpringApplication.run(YbConsumerApplication.class, args);
    }


}
