package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class SimpleProducerAsync {

    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerAsync.class);

    public static void main(String[] args){
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

        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName,  "Hello World asyn c");

        //KafkaProducer message send

        // callback은 sendThread에서 호출한다.
        producer.send(record, (metadata, exception) -> {
            if(exception == null){
                logger.info("\n ###### record metadata received ###### \npartition:{}\noffset:{}\ntimestamp:{}", metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                logger.error("exception error from broker {}", exception.getMessage());
            }
        });

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
