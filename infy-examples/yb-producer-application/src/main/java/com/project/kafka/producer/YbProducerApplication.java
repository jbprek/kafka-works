
package com.project.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@SpringBootApplication
@Slf4j
public class YbProducerApplication implements CommandLineRunner {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String topicName = "ybtopic";

    /*
     * Configure & Create Kafka Producer
     */
    private static KafkaProducer<String, String> createProducer() {
        Properties propsClickStream = new Properties();
        propsClickStream.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        propsClickStream.put(ProducerConfig.CLIENT_ID_CONFIG, "ClickStreamProducer");
        propsClickStream.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        propsClickStream.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(propsClickStream);
    }


    @Override
    public void run(String... args) {
        if (args.length < 1) {
            throw new IllegalStateException("Expecting 1 args, 0 : CSV file name");
        }

        String filename = args[0];

        KafkaProducer<String, String> producer = createProducer();

        Function<String, String> csvFirstColumn = (csv) -> csv.split(Pattern.quote(","))[0];
        Function<String, ProducerRecord<String, String>> mapper = (csv) -> new ProducerRecord<String, String>(topicName, csvFirstColumn.apply(csv), csv);
        Consumer<ProducerRecord<String, String>> publish = w -> {
            try {
                Future<RecordMetadata> future = producer.send(w);
                RecordMetadata meta = future.get();
                log.info("Published record:  timestamp - {} partition - {} - offset {} ", meta.timestamp(), meta.partition(), meta.offset());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        };
        try (Stream<String> lines = Files.lines(Paths.get(filename))) {
            lines.map(mapper)
                    .peek(w -> log.info("Publishing record {} ", w))
                    .forEach(publish);
        } catch (IOException e) {
            log.error("Failed to process file :" + filename);
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        SpringApplication.run(YbProducerApplication.class, args);
    }


}
