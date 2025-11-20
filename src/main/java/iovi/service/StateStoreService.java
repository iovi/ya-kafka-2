package iovi.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Slf4j
@Service
@RequiredArgsConstructor
public class StateStoreService {
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

    private final SettingsProducerService settingsProducerService;

    private KafkaStreams streams2;

    @PostConstruct
    public void setStreams() {
        Properties props2 = new Properties();
        props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        StreamsBuilder builder2 = new StreamsBuilder();
        // Создаем KTable из топика блокированных пользователей
        KTable<Long, Long> blockedUsersTable = builder2.table(
                blockedUsersTopic,
                Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as(blockedUsersStoreName)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.Long()));
        streams2 = new KafkaStreams(builder2.build(), props2);
        streams2.start();
    }

    @PreDestroy
    public void close() {
        streams2.close();
    }

    public void configureBlockedUsers() {
        try {
            // Заполняем данными
            settingsProducerService.produceBlockedUsers();

            // Ждём, пока данные обработаются
            waitForStateStoreToBeReady();

            // Показываем имеющиеся данные
            printStateStore(blockedUsersStoreName);
        } catch (Throwable e) {
            log.error("Ошибка в приложении: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    public void waitForStateStoreToBeReady() throws InterruptedException {
        final long MAX_WAIT_MS = 60000;
        final long RETRY_INTERVAL_MS = 1000;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + MAX_WAIT_MS;

        while (System.currentTimeMillis() < endTime) {
            if (streams2.state() == KafkaStreams.State.RUNNING) {
                try {
                    streams2.store(StoreQueryParameters.fromNameAndType(
                            blockedUsersStoreName, QueryableStoreTypes.keyValueStore()));
                    log.info("State store {} готово к запросам", blockedUsersStoreName);
                    return;
                } catch (Exception e) {
                    log.info("Ожидание готовности state store... (" +
                            (System.currentTimeMillis() - startTime) / 1000 + " сек)");
                }
            } else {
                log.info("Ожидание состояния RUNNING... Текущее состояние: " + streams2.state());
            }

            Thread.sleep(RETRY_INTERVAL_MS);
        }
        log.error("Превышено время ожидания готовности state store {}", blockedUsersStoreName);
    }

    private void printStateStore(String storeName) {
        try {
            ReadOnlyKeyValueStore<Long, Long> store = streams2.store(
                    StoreQueryParameters.fromNameAndType(blockedUsersStoreName, QueryableStoreTypes.keyValueStore())
            );

            KeyValueIterator<Long, Long> iterator = store.all();
            boolean isEmpty = true;

            while (iterator.hasNext()) {
                isEmpty = false;
                KeyValue<Long, Long> entry = iterator.next();

                log.info("key: {}   value: {}", entry.key, entry.value);
            }

            if (isEmpty) {
                log.warn("state store {} is empty", storeName);
            }
            iterator.close();
        } catch (Exception e) {
            log.error("Error while printing {} ", storeName, e);
        }
    }

    public ReadOnlyKeyValueStore<Long, Long> getBlockedUsersStore() {
        return streams2.store(
                StoreQueryParameters.fromNameAndType(blockedUsersStoreName, QueryableStoreTypes.keyValueStore()));
    }

}
