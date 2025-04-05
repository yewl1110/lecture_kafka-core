package com.example.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerPartitionAssign {

    public static final Logger logger = LoggerFactory.getLogger(ConsumerPartitionAssign.class);

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.254.64:19093");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group_pizza_assign_seek");
//        props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "6000");
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        consumer.assign(List.of(topicPartition));

        // main thread 참조 변수
        Thread mainThread = Thread.currentThread();

        // main thread 종료시 별도의 thread로 kafka consumer의 wakeup() 메서드 호출
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("main program starts to exit by calling wakeup");
            consumer.wakeup();

            try {
                mainThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));


//        pollAutoCommit(consumer);
        pollCommitSync(consumer);
//        pollCommitAsync(consumer);
    }

    private static void pollCommitAsync(KafkaConsumer<String, String> consumer) {
        try {
            int loopCount = 0;

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                logger.info("loopCnt:{}, consumerRecords count: {}", loopCount++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition: {}, record offset: {}, record value:{}", record.key(), record.partition(), record.offset(), record.value());
                }
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            logger.error("offsets {} is not completed, error:{}", offsets, exception.getMessage());
                        }
                    }
                });
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            consumer.commitSync();
            logger.info("Closing consumer");
            consumer.close();
        }
    }

    private static void pollCommitSync(KafkaConsumer<String, String> consumer) {
        try {
            int loopCount = 0;

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                logger.info("loopCnt:{}, consumerRecords count: {}", loopCount++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition: {}, record offset: {}, record value:{}", record.key(), record.partition(), record.offset(), record.value());
                }
                try {
                    if (consumerRecords.count() > 0) {
                        consumer.commitSync();
                        logger.info("commit sync has been called");
                    }
                } catch (CommitFailedException e) {
                    logger.error(e.getMessage());
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            logger.info("Closing consumer");
            consumer.close();
        }
    }

    private static void pollAutoCommit(KafkaConsumer<String, String> consumer) {

        try {
            int loopCount = 0;

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                logger.info("loopCnt:{}, consumerRecords count: {}", loopCount++, consumerRecords.count());
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    logger.info("record key:{}, partition: {}, record offset: {}, record value:{}", record.key(), record.partition(), record.offset(), record.value());
                }

                try {
                    logger.info("main thread is sleeping {} ms during while loop", 10000);
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (WakeupException e) {
            logger.error("wakeup exception has been called");
        } finally {
            logger.info("Closing consumer");
            consumer.close();
        }
    }
}
