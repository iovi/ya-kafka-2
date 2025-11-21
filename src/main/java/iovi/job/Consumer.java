package iovi.job;

import iovi.dto.MessageDto;
import iovi.serdes.MessageDtoDeserializer;
import iovi.service.UserService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class Consumer {
    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @Value("${my.kafka.out.topic.prefix}")
    private String outTopicPrefix;

    private KafkaConsumer<String, MessageDto> consumer;

    private final String workingPeriodMs = "1000";

    private final UserService userService;

    @PostConstruct
    public void setUpConsumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //ключ десериализуется как строка
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDtoDeserializer.class.getName()); //значение десериализуется кастомно
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true"); // применение offset автоматическое
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, workingPeriodMs);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        consumer = new KafkaConsumer<>(properties);


        //подписываемся на все
        consumer.subscribe(userService.getUsers().stream().map(u -> outTopicPrefix + u.getId())
                .collect(Collectors.toList()));
    }

    @PreDestroy
    public void closeProducer() {
        consumer.close();
    }

    @Scheduled(fixedDelayString = workingPeriodMs)
    public void getMessages() {
        try {
            ConsumerRecords<String, MessageDto> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, MessageDto> record : records) {
                log.info("from {} consumed: {}", record.topic(), record.value());
            }
        } catch (Exception e) {
            log.error("Exception occurs: {}", e.getMessage());
        }
    }
}
