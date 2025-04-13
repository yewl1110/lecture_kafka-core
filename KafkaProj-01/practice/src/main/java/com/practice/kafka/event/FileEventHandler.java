package com.practice.kafka.event;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class FileEventHandler implements EventHandler {

    public static final Logger logger = LoggerFactory.getLogger(FileEventHandler.class);
    private final KafkaProducer<String, String> kafkaProducer;
    private final String topicName;
    private final boolean sync;

    public FileEventHandler(KafkaProducer<String, String> kafkaProducer, String topicName, boolean sync) {
        this.kafkaProducer = kafkaProducer;
        this.topicName = topicName;
        this.sync = sync;
    }

    @Override
    public void onMessage(MessageEvent messageEvent) throws InterruptedException, ExecutionException {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, messageEvent.key, messageEvent.value);
        if(sync) {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ###### record metadata received ###### \npartition:{}\noffset:{}\ntimestamp:{}", recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
        } else {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if(exception == null){
                    logger.info("\n ###### record metadata received ###### \npartition:{}\noffset:{}\ntimestamp:{}", metadata.partition(), metadata.offset(), metadata.timestamp());
                } else {
                    logger.error("exception error from broker {}", exception.getMessage());
                }
            });
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        String topicName = "file-topic";
        boolean sync = true;

        FileEventHandler fileEventHandler = new FileEventHandler(kafkaProducer, topicName, sync);
        fileEventHandler.onMessage(new MessageEvent("key00001", "test! test! test!"));
    }
}
