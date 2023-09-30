package org.example.producer;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; // 연동할 브로커 명과 포트번호는 2개 이상

    public static void main(String[] args) {

        // 필수 옵션 부트스트랩, 메시지 키와 값의 직렬화 옵션
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 프로듀서 인스턴스 생성
        // 연결할 브로커와 직렬화 옵션 설정
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);
        // 전송 준비

        // 레코드 생성
        String messageValue = "testMessage";
        // 토픽 명과 메시지 값
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue);
        // send 메서드 호출
        producer.send(record);
        logger.info("{}", record);
        producer.flush(); // 데이터 강제로 날리기
        producer.close(); // 프로듀서 종료

    }
}