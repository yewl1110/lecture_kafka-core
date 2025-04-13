package com.practice.kafka.producer;

import com.practice.kafka.event.EventHandler;
import com.practice.kafka.event.FileEventHandler;
import com.practice.kafka.event.FileEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Properties;

public class FileAppendProducer {

    public static final Logger logger = LoggerFactory.getLogger(FileAppendProducer.class);

    public static void main(String[] args) {
        String topicName = "file-topic";

        //KafkaProducer configuration setting

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        // key, value 타입 = String, String
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        boolean sync = false;

        File file = new File("D:\\GitHub\\lecture_kafka-core\\KafkaProj-01\\practice\\src\\main\\resources\\pizza_append.txt");

        EventHandler fileEventHandler = new FileEventHandler(producer, topicName, sync);
        FileEventSource fileEventSource = new FileEventSource(1000, file, fileEventHandler);
        Thread fileEventSourceThread = new Thread(fileEventSource);
        fileEventSourceThread.start();

        try {
            fileEventSourceThread.join(); // 메인스레드는 fileEventSourceThread가 죽을 때 까지 기다린다.
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        } finally {
            producer.close();
        }
    }
}
