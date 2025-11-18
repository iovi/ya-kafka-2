package iovi.kafka;


import iovi.dto.MessageDto;
import iovi.serdes.MessageDtoSerdes;
import iovi.service.UserService;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class Streams implements CommandLineRunner {

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @Value("${my.kafka.in.topic}")
    private String inTopic;

    @Value("${my.kafka.out.topic.prefix}")
    private String outTopicPrefix;

    private final String blockedUsersStore = "blocked_users" ;

    private final UserService userService;


    @Override
    public void run(String... args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageDtoSerdes.class);


        StreamsBuilder builder = new StreamsBuilder();



        // Создание Persistent State Store
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(blockedUsersStore),
                Serdes.Long(),
                Serdes.ListSerde(ArrayList.class, Serdes.Long()));
        builder.addStateStore(storeBuilder);

        KStream<String, MessageDto> stream = builder.stream(inTopic,
                Consumed.with(Serdes.String(), new MessageDtoSerdes()));


        // Топология
        //каждому пользователю отправляем в свой выходной поток
        userService.getUsers().forEach(u -> {
            KStream<String, MessageDto> filteredStream = stream.filter(
                    (key, value) -> value.getUserId().equals(u.getId()));
            filteredStream.to(outTopicPrefix + u.getId());
        });

        // Запускаем приложение
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        try {
            streams.start();
            System.out.println("Приложение запущено");

            // Демонстрация запросов к state store
            //Thread.sleep(15000); // Даем время на обработку сообщений

        } catch (Throwable e) {
            System.err.println("Ошибка при запуске приложения: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
