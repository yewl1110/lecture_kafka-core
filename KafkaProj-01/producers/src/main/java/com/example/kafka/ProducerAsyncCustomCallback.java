package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncCustomCallback {

    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncCustomCallback.class);

    public static void main(String[] args){
        String topicName = "multipart-topic";

        //KafkaProducer configuration setting

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        // key, value 타입 = String, String
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);


        for(int seq = 0; seq < 20; seq++) {

            // ProducerRecord 객체 생성.
            // Properties에서 key,value serializer의 타입, KafkaProducer의 key,value 타입, ProducerRecord의 key,value 타입 모두 일치해야 함

            ProducerRecord<Integer, String> record = new ProducerRecord<>(topicName,  seq, "Hello World " + seq);

            //KafkaProducer message send

            Callback callback = new CustomCallback(seq);
            // callback은 sendThread에서 호출한다.
            producer.send(record, callback);

        }

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
