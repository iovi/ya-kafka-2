package iovi.serdes;

import iovi.dto.MessageDto;
import org.apache.kafka.common.serialization.Serdes;

public class MessageDtoSerdes extends Serdes.WrapperSerde<MessageDto> {
    public MessageDtoSerdes() {
        super(new MessageDtoSerializer(), new MessageDtoDeserializer());
    }
}