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
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
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

    @Value("${my.kafka.blocked.users.topic}")
    private String blockedUsersTopic;

    private final String blockedUsersStore = "blocked_users" ;

    private final UserService userService;


    @Override
    public void run(String... args) throws Exception {
        setUpOutputTopics();


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MessageDtoSerdes.class);

        StreamsBuilder builder = new StreamsBuilder();



        // Создание Persistent State Store
// Настройка Kafka Streams
        Properties props1 = new Properties();
        props1.put(StreamsConfig.APPLICATION_ID_CONFIG, "ya-kafka-2");
        props1.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props1.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props1.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());

        StreamsBuilder builder2 = new StreamsBuilder();

        // Создаем KTable из топика блокированных пользователей
        KTable<Long, Long> blockedUsersTable = builder2.table(
                blockedUsersTopic,
                Materialized.<Long, Long, KeyValueStore<Bytes, byte[]>>as(blockedUsersStore)
                        .withKeySerde(Serdes.Long())
                        .withValueSerde(Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder2.build(), props1);
        streams.start();
        final CountDownLatch latch = new CountDownLatch(1);

        // Обработка завершения работы
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
            latch.countDown();
        }));

        waitForStateStoreToBeReady(streams);

        // Показываем текущий каталог товаров
        System.out.println("\nТекущий каталог товаров:");
        printProductCatalog(streams);


        KStream<String, MessageDto> stream = builder.stream(inTopic,
                Consumed.with(Serdes.String(), new MessageDtoSerdes()));


        // Топология
        //каждому пользователю отправляем в свой выходной поток
        userService.getUsers().forEach(u -> {
            //не шлём сами себе
            KStream<String, MessageDto> filteredStream2 = stream.filter(
                    (key, value) -> !value.getUserId().equals(u.getId()));
            filteredStream2.to(outTopicPrefix + u.getId());
        });

        // Запускаем приложение
        KafkaStreams streams1 = new KafkaStreams(builder.build(), props);

        try {
            streams1.start();
            System.out.println("Приложение запущено");

            // Демонстрация запросов к state store
            //Thread.sleep(15000); // Даем время на обработку сообщений

        } catch (Throwable e) {
            System.err.println("Ошибка при запуске приложения: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void setUpOutputTopics(){
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

    private void waitForStateStoreToBeReady(KafkaStreams streams) throws InterruptedException {
        final long MAX_WAIT_MS = 60000;
        final long RETRY_INTERVAL_MS = 1000;

        long startTime = System.currentTimeMillis();
        long endTime = startTime + MAX_WAIT_MS;

        while (System.currentTimeMillis() < endTime) {
            if (streams.state() == KafkaStreams.State.RUNNING) {
                try {
                    streams.store(StoreQueryParameters.fromNameAndType(
                            blockedUsersStore, QueryableStoreTypes.keyValueStore()));
                    System.out.println("State store готово к запросам");
                    return;
                } catch (Exception e) {
                    System.out.println("Ожидание готовности state store... (" +
                            (System.currentTimeMillis() - startTime) / 1000 + " сек)");
                }
            } else {
                System.out.println("Ожидание состояния RUNNING... Текущее состояние: " + streams.state());
            }

            Thread.sleep(RETRY_INTERVAL_MS);
        }

        throw new RuntimeException("Превышено время ожидания готовности state store");
    }

    private void printProductCatalog(KafkaStreams streams) {
        try {
            ReadOnlyKeyValueStore<Long, Long> store = streams.store(
                    StoreQueryParameters.fromNameAndType(blockedUsersStore, QueryableStoreTypes.keyValueStore())
            );

            KeyValueIterator<Long, Long> iterator = store.all();
            boolean isEmpty = true;

            while (iterator.hasNext()) {
                isEmpty = false;
                KeyValue<Long, Long> entry = iterator.next();

                log.info("ID: {} blocked id {}", entry.key , entry.value);
            }

            if (isEmpty) {
                System.out.println("Каталог пуст.");
            }

            iterator.close();
        } catch (Exception e) {
            System.err.println("Ошибка при чтении каталога: " + e.getMessage());
        }
    }

    private void settingProducer(){
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Long.class.getName()); //ключ сериализуется как строка
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Long.class.getName());

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
        try( KafkaProducer<Long, Long> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<Long, Long> record12 = new ProducerRecord<>(blockedUsersTopic, 1L, 2L);
            producer.send(record12);
            //ProducerRecord<Long, Long> record13 = new ProducerRecord<>(blockedUsersTopic, 1L, 3L);
            //producer.send(record13);
            ProducerRecord<Long, Long> record24 = new ProducerRecord<>(blockedUsersTopic, 2L, 4L);
            producer.send(record24);
        } catch (Exception e) {
            log.error("Exception ", e);
        }
    }
}


