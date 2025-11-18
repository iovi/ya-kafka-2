package iovi.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import iovi.dto.MessageDto;
import org.apache.kafka.common.serialization.Deserializer;

public class MessageDtoDeserializer implements Deserializer<MessageDto> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public MessageDto deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, MessageDto.class);
        } catch (Exception e) {
            throw new RuntimeException("Ошибка десериализации объекта MessageDto", e);
        }
    }
}