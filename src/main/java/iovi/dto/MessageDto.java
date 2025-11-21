package iovi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MessageDto {

    /**Уникальный ид. сообщения*/
    private String uuid;

    /**Текст сообщения*/
    private String messageText;

    /**Ид. пользователя-отправителя*/
    private Long userId;
}
