package iovi.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Сервис, который отправляет справочные данные в топики
 */
@Slf4j
@Service
public class SettingsProducerService {

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @Value("${my.kafka.blocked.users.topic}")
    private String blockedUsersTopic;

    /**
     * Генерация данных о заблокированных польлзователях
     */
    public void produceBlockedUsers() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());

        //создадим топик для заблокированных пользователей
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaAddress);
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            NewTopic inputTopic = new NewTopic(blockedUsersTopic, 1, (short) 1);
            adminClient.createTopics(List.of(inputTopic)).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("topic creation error " + e.getMessage());
        }

        //отправим перечни заблокированных
        try (KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<Long, Long> record12 = new ProducerRecord<>(blockedUsersTopic, 1L, 2L);
            producer.send(record12);
            //ProducerRecord<Long, Long> record13 = new ProducerRecord<>(blockedUsersTopic, 1L, 3L);
            //producer.send(record13);
            ProducerRecord<Long, Long> record24 = new ProducerRecord<>(blockedUsersTopic, 2L, 4L);
            producer.send(record24);
            producer.flush();
        } catch (Exception e) {
            log.error("Exception ", e);
        }
    }
}
