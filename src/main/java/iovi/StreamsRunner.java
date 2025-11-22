package iovi;


import iovi.dto.MessageDto;
import iovi.serdes.MessageDtoSerdes;
import iovi.service.BadWordsService;
import iovi.service.StateStoreService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
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

    @Value("${my.kafka.out.topic}")
    private String outTopic;

    @Value("${my.kafka.blocked.users.topic}")
    private String blockedUsersTopic;

    @Value("${my.kafka.blocked.users.store}")
    private String blockedUsersStoreName;

    private final StateStoreService stateStoreService;

    private final BadWordsService badWordsService;

    @Override
    public void run(String... args) {
        setUpOutputTopics();
        stateStoreService.configureBlockedUsers();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageDtoSerdes.class);

        StreamsBuilder builder = new StreamsBuilder();

        // таблица с заблокированными пользователями
        KTable<Long, Long> blockedUsersTable = builder.table(
                blockedUsersTopic,
                Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as(blockedUsersStoreName)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.Long()));

        // получаем из входного топика
        KStream<Long, MessageDto> inputStream = builder.stream(inTopic);

        // добавляем информацию о заблокированных полльзователях.
        // Предполагается, что пользователи сами не будут получать не предназначенные им сообщения :)
        KStream<Long, MessageDto> blockedUsersStream = inputStream.leftJoin(blockedUsersTable,
                (message, blockedUser) -> {
                    message.getUserIdsBlackList().add(blockedUser);
                    return message;
                });

        // маскируем запрещённые слова
        KStream<Long, MessageDto> maskedStream = blockedUsersStream.mapValues(m ->
                new MessageDto(badWordsService.mask(m.getMessageText()), m.getUserId(), m.getUserIdsBlackList()));

        //направляем всё в выходной топик
        maskedStream.to(outTopic);


        // Инициализация и запуск Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        System.out.println("Kafka Streams приложение запущено успешно.");
    }

    private void setUpOutputTopics() {
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", kafkaAddress);
        // создаём исходящий топик
        try (AdminClient adminClient = AdminClient.create(adminProps)) {
            List<NewTopic> topics = new ArrayList<>();
            NewTopic outputTopic = new NewTopic(outTopic, 1, (short) 1);
            topics.add(outputTopic);
            adminClient.createTopics(topics).all().get();
        } catch (InterruptedException | ExecutionException e) {
            log.warn("topic creation error " + e.getMessage());
        }
    }
}


