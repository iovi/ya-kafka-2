package iovi.dto;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class MessageDto {

    private String uuid;

    private String messageText;

    private Long userId;
}
