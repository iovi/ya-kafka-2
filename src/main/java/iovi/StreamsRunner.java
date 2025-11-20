package iovi;


import iovi.dto.MessageDto;
import iovi.serdes.MessageDtoSerdes;
import iovi.service.StateStoreService;
import iovi.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
@RequiredArgsConstructor
public class StreamsRunner implements CommandLineRunner {

    @Value("${my.kafka.address}")
    private String kafkaAddress;

    @Value("${my.kafka.in.topic}")
    private String inTopic;

    @Value("${my.kafka.out.topic.prefix}")
    private String outTopicPrefix;

    @Value("${my.kafka.blocked.users.topic}")
    private String blockedUsersTopic;

    @Value("${my.kafka.blocked.users.store}")
    private String blockedUsersStoreName;

    private final UserService userService;

    private final StateStoreService stateStoreService;


    @Override
    public void run(String... args)  {
        setUpOutputTopics();
        stateStoreService.configureBlockedUsers();


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageDtoSerdes.class);

        // Создание топологии
        StreamsBuilder builder = new StreamsBuilder();

        KTable<Long, Long> blockedUsersTable = builder.table(
                blockedUsersTopic,
                Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as(blockedUsersStoreName)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.Long()));


        KStream<String, MessageDto> inputStream = builder.stream(inTopic);

        //каждому пользователю отправляем в свой выходной поток
        userService.getUsers().forEach(u -> {
            ReadOnlyKeyValueStore<Long, Long> blockedUsersStore = stateStoreService.getBlockedUsersStore();
            KStream<String, MessageDto> filteredStream = inputStream.filter((key, value) ->
                    //не шлём сами себе
                    !value.getUserId().equals(u.getId())
                    //и не шлём, если отправитель заблокирован для данного пользователя
                    && !value.getUserId().equals(blockedUsersStore.get(u.getId()))
            );


            filteredStream.to(outTopicPrefix + u.getId());
        });

        // Инициализация и запуск Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Kafka Streams приложение запущено успешно.");
    }

    private void setUpOutputTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaAddress);
        // У каждого пользователя свой топик для получения в виде префикса , создаём их
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();
            userService.getUsers().forEach(u -> {
                String topicName = outTopicPrefix + u.getId();
                NewTopic outputTopic = new NewTopic(topicName, 1, (short) 1);
                topics.add(outputTopic);
            });
            adminClient.createTopics(topics).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("topic creation error " + e.getMessage());
        }
    }
}


