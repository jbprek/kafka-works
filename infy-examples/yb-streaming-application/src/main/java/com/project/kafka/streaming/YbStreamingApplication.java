package com.project.kafka.streaming;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.regex.Pattern;

@SpringBootApplication
@Slf4j
public class YbStreamingApplication implements CommandLineRunner {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String inputTopic = "ybtopic";
    private static final String outputTopic = "ybTruant";


    @Override
    public void run(String... args) {
        Properties propsClickStream = new Properties();
        propsClickStream.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        propsClickStream.put(StreamsConfig.APPLICATION_ID_CONFIG, "ClickStreamAnalysis");
        propsClickStream.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        propsClickStream.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        StreamsBuilder streamBuilder = new StreamsBuilder();
        KStream<String, String> inputStream = streamBuilder.stream(inputTopic);

        KStream<String, String> filteredStream = inputStream.filter(
                new Predicate<String, String>() {
                    @Override
                    public boolean test(String key, String value) {
                        String[] splitValues = value.split(Pattern.quote(","));
                        LocalDate  startDate = LocalDate.parse(splitValues[1]);
                        LocalTime  startTime = LocalTime.parse(splitValues[2]);
                        LocalDateTime startDateTime = LocalDateTime.of(startDate, startTime);
                        LocalDate  endDate = LocalDate.parse(splitValues[4]);
                        LocalTime  endTime = LocalTime.parse(splitValues[5]);
                        LocalDateTime endDateTime = LocalDateTime.of(endDate, endTime);
                        boolean isTruant = ChronoUnit.MINUTES.between(startDateTime, endDateTime) < 8 * 60;
                        if (isTruant )
                            log.info("Truant value : {}", value);

                        return isTruant;
                    }
                }
        );

        filteredStream.to(outputTopic);

        KafkaStreams streams = new KafkaStreams(streamBuilder.build(), propsClickStream);

        streams.start();
    }

    public static void main(String[] args) {
        SpringApplication.run(YbStreamingApplication.class, args);
    }


}
