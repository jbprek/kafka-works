/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.project.kafka.producer;

import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Pattern;
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
public class SpringBootConsoleApplication implements CommandLineRunner {

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
        SpringApplication.run(SpringBootConsoleApplication.class, args);
    }

    @Override
    public void run(String... args) {
		if (args.length < 2) {
			throw new IllegalStateException("Expecting 2 args, 0 : CSV string, 1:topicName");
		}
    	String topic = args[1];
        KafkaProducer<String, String> producer = createProducer();
        Scanner streamInput = new Scanner(args[0]);
        while (streamInput.hasNextLine()) {
            String record = streamInput.nextLine();
            String[] columns = record.split(Pattern.quote(","));
			final ProducerRecord<String, String> event = new ProducerRecord<>(topic, columns[0], record);
			producer.send(event);
        }
    }

}
