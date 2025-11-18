package iovi.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import iovi.dto.MessageDto;
import org.apache.kafka.common.serialization.Serializer;

public class MessageDtoSerializer implements Serializer<MessageDto> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize (String topic, MessageDto messageDto){
        if (messageDto == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsBytes(messageDto);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Ошибка сериализации объекта MessageDto", e);
        }
    }
}

