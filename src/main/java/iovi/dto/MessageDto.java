package iovi.dto;

import lombok.Data;
import lombok.Getter;
import lombok.ToString;

@Data
@ToString
public class MessageDto {

    private String uuid;

    private String word;

    private Long userId;
}
