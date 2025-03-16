package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String[] args) {
        String topicName = "simple-topic";

        //KafkaProducer configuration setting

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        // key, value 타입 = String, String
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        // ProducerRecord 객체 생성.
        // Properties에서 key,value serializer의 타입, KafkaProducer의 key,value 타입, ProducerRecord의 key,value 타입 모두 일치해야 함

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,  "Hello World sunday");

        //KafkaProducer message send
        // thread로 send (비동기)
        producer.send(record);


        // 메세지를 batch 단위로 보내기 때문에 버퍼를 사용하는데, 종료 할때는 지워줘야 한다
        producer.flush();
        producer.close();
    }
}
