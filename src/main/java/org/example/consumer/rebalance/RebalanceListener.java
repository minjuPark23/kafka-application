package org.example.consumer.rebalance;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

// consumerRebalanceListener 상속
public class RebalanceListener implements ConsumerRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    // AFTER
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 토픽 파티션에 대한 컬랙션
        // 리밸런스 할당 후 현재 컨슈머 스레드에 할당된 토픽 파티션에 대한 정보를 갖는다.
        logger.warn("Partitions are assigned : " + partitions.toString());

    }

    // BEFORE
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 토픽 파티션에 대한 컬랙션
        // 리밸런스 할당 잔 컨슈머 스레드에 할당되었던 토픽 파티션에 대한 정보를 갖는다.
        logger.warn("Partitions are revoked : " + partitions.toString());
    }
}