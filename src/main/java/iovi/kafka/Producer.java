package iovi.kafka;

import iovi.dto.MessageDto;
import iovi.dto.User;
import iovi.service.UserService;
import iovi.util.RandomMessageUtilService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
@AllArgsConstructor
public class Producer {

    private final UserService userService;

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    private KafkaProducer<String, MessageDto> producer;

    private final Random random = new Random();

    @PostConstruct
    public void setUpProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //ключ сериализуется как строка
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName()); //значение сериализуется как json
        producer = new KafkaProducer<>(properties);
    }

    @PreDestroy
    public void closeProducer() {
        producer.close();
    }

    @Scheduled(fixedDelay = 2000)
    public void sendRecord() {
        //сообщения будет слать каждый пользователь
        userService.getUsers().forEach( u-> {
            //создание сообщения
            MessageDto messageDto = new MessageDto();
            messageDto.setUuid(UUID.randomUUID().toString());
            messageDto.setWord(RandomMessageUtilService.getRandomWord());
            messageDto.setUserId(u.getId());

            // отправка сообщения
            ProducerRecord<String, MessageDto> record = new ProducerRecord<>("word_topic", messageDto.getUuid(),
                    messageDto);
            try {
                producer.send(record, (metadata, e) -> {
                    if (e == null) {
                        log.info("Produced and sent: {}", messageDto);
                    } else {
                        log.error("Error sending message {}: {}", messageDto, e.getMessage());
                    }
                });
            } catch (Exception e) {
                log.error("Exception for message {}: {}", messageDto, e.getMessage());
            }
        });
    }
}
