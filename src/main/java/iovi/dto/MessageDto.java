package iovi.dto;

import lombok.Data;
import lombok.Getter;

@Data
public class MessageDto {

    private String uuid;

    private String word;

    private Long userId;
}
