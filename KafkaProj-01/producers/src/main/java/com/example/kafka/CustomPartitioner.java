package com.example.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class);
    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKeyName;

    @Override
    public void configure(Map<String, ?> configs) {
        specialKeyName = configs.get("custom.specialKey").toString();
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // cluster는 브로커에 있는 정보들을 해싱해서 가져옴
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int numPartitions = partitionInfos.size();
        int numSpecialPartitions= numPartitions / 2;
        int partitionIndex = 0;

        if (keyBytes == null) {
            throw new InvalidRecordException("keyBytes is null");
         }

        if(String.valueOf(key).equals(specialKeyName)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % ( numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }
        logger.info("key:{} is sent to partition:{}", key, partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
