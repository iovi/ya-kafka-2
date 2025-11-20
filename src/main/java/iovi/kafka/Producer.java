package iovi.kafka;

import iovi.dto.MessageDto;
import iovi.serdes.MessageDtoSerializer;
import iovi.service.UserService;
import iovi.util.RandomMessageUtilService;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class Producer {

    private final UserService userService;

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @Value("${my.kafka.in.topic}")
    private String inTopic;

    private KafkaProducer<String, MessageDto> producer;

    @PostConstruct
    public void setUpProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //ключ сериализуется как строка
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageDtoSerializer.class.getName());
        producer = new KafkaProducer<>(properties);

        //создадим топик
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaAddress);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic inputTopic = new NewTopic(inTopic, 1, (short) 1);
            adminClient.createTopics(List.of(inputTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("topic creation error " + e.getMessage());
        }

    }

    @PreDestroy
    public void closeProducer() {
        producer.close();
    }

    @Scheduled(fixedDelay = 10000)
    public void sendRecord() {
        //сообщения будет слать каждый пользователь
        userService.getUsers().forEach( u-> {
            //создание сообщения
            MessageDto messageDto = new MessageDto();
            messageDto.setUuid(UUID.randomUUID().toString());
            messageDto.setWord(RandomMessageUtilService.getRandomWord());
            messageDto.setUserId(u.getId());

            // отправка сообщения
            ProducerRecord<String, MessageDto> record = new ProducerRecord<>(inTopic, messageDto.getUuid(),
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
