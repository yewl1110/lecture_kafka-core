package com.practice.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class FileProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileProducer.class);

    public static void main(String[] args) {
        String topicName = "file-topic";

        //KafkaProducer configuration setting

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        // key, value 타입 = String, String
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String filePath = "D:\\GitHub\\lecture_kafka-core\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_sample.txt";

        sendFileMessages(producer, topicName, filePath);

        producer.close();
    }

    private static void sendFileMessages(KafkaProducer<String, String> producer, String topicName, String filePath) {
        String line;
        final String delimiter = ",";
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            while ((line = bufferedReader.readLine()) != null) {
                String[] tokens = line.split(delimiter);
                String key = tokens[0];
                StringBuffer value = new StringBuffer();

                for (int i = 1; i < tokens.length; i++) {
                    if (i != tokens.length - 1) {
                        value.append(tokens[i] + delimiter);
                    } else {
                        value.append(tokens[i]);
                    }
                }
                sendMessage(producer, topicName, key, value.toString());
            }

        } catch (IOException e) {
            logger.info(e.getMessage());
        }
    }

    private static void sendMessage(KafkaProducer<String, String> producer, String topicName, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
        logger.info("key:{} value:{}", key, value);

        producer.send(record, (metadata, exception) -> {
            if (exception == null) {
                logger.info(
                        "\n ###### record metadata received ###### \npartition:{}\noffset:{}\ntimestamp:{}",
                        metadata.partition(),
                        metadata.offset(),
                        metadata.timestamp()
                );
            } else {
                logger.error("exception error from broker {}", exception.getMessage());
            }
        });
    }
}
