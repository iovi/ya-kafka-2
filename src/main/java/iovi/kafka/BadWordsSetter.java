package iovi.kafka;

import iovi.dto.MessageDto;
import iovi.service.BadWordsService;
import iovi.util.RandomMessageUtilService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class BadWordsSetter {

    private final BadWordsService badWordsService;

    @Scheduled(fixedDelay = 600000)
    public void setBadWord() {
        String word = RandomMessageUtilService.getRandomWord();
        log.info("Set bad word {} !!!!!!", word);
        badWordsService.addBadWord(word);
    }
}
