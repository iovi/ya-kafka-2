package iovi.service;

import org.apache.kafka.common.protocol.types.Field;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class BadWordsService {

    private Set<String> badWords = new HashSet<>();

    public void addBadWord(String word) {
        badWords.add(word);
    }

    public void removeBadWord(String word) {
        badWords.remove(word);
    }

    public boolean isBad(String word) {
        return badWords.contains(word);
    }

    public String mask(String text){
        return Arrays.stream(text.split(" ")).map(s-> isBad(s) ? "***" : s)
                .collect(Collectors.joining(" "));
    }
}
