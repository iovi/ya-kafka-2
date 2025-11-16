package iovi.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class RandomMessageUtilService {

    private static final List<String> DICTIONARY = Arrays.asList("apple", "banana", "cherry", "elderberry",
            "orange", "grape", "kiwi", "melon");

    public static String getRandomWord() {
        Random random = new Random();
        int randomIndex = random.nextInt(DICTIONARY.size());
        return DICTIONARY.get(randomIndex);
    }
}
