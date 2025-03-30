package com.example.kafka;

import com.github.javafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {

    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class);

    public static void sendPizzaMessage(
            KafkaProducer<String, String> producer,
            String topicName,
            int iterCount,
            int interIntervalMillis,
            int intervalMillis,
            int intervalCount,
            boolean sync
    ) {
        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String,String> record = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));
            sendMessage(producer, record, pMessage, sync);

            if((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("####### intervalCount:{} intervalMillis:{}", intervalCount, intervalMillis);
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if(interIntervalMillis > 0) {
                try {
                    logger.info("####### interIntervalMillis:{}", interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            iterSeq++;
        }

    }

    public static void sendMessage(
            KafkaProducer<String, String> producer,
            ProducerRecord<String, String> record,
            HashMap<String, String> pMessage,
            boolean sync
    ) {
        if (!sync) {
            producer.send(record, (metadata, exception) -> {
                if(exception == null){
                    logger.info(
                            "async message: {} partition:{} offset:{}", pMessage.get("key"), metadata.partition(), metadata.offset()
                    );
                } else {
                    logger.error("exception error from broker {}", exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = producer.send(record).get();
                logger.info(
                        "sync message: {} partition:{} offset:{}", pMessage.get("key"), metadata.partition(), metadata.offset()
                );
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            } catch (ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args){
        String topicName = "pizza-topic";

        //KafkaProducer configuration setting

        Properties props = new Properties();
        //bootstrap.servers, key.serializer.class, value.serializer.class
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");
//        props.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "6");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        // KafkaProducer 객체 생성
        // key, value 타입 = String, String
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        sendPizzaMessage(producer, topicName, -1, 1000, 0, 0, true);

        producer.close();
    }
}
