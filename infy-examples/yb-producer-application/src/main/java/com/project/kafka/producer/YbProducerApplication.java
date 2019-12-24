
package com.project.kafka.producer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Scanner;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class YbProducerApplication implements CommandLineRunner {

    private static final String BOOTSTRAP_SERVERS = "192.168.99.100:9092";

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

    public static void main(String[] args) {
        SpringApplication.run(YbProducerApplication.class, args);
    }

    @Override
    public void run(String... args) {
		if (args.length < 2) {
			throw new IllegalStateException("Expecting 2 args, 0 : CSV file name, 1:topicName");
		}
    	String topic = args[1];
        KafkaProducer<String, String> producer = createProducer();
        String filename = args[0];

        Function<String, String> csvFirstColumn = (csv) ->  csv.split(Pattern.quote(","))[0];
        Function<String, ProducerRecord<String, String>> mapper = (csv) ->  new ProducerRecord<String, String>(topic, csvFirstColumn.apply(csv), csv );

        try (Stream<String> lines = Files.lines(Paths.get(filename))) {
            lines.map(csv -> mapper.apply(csv))
                    .peek(w->log.info("Publish event {} ", w))
                    .forEach(event->producer.send(event));
        } catch (IOException e) {
            log.error("Failed to process file :"+filename);
            e.printStackTrace();
        }

    }

}
