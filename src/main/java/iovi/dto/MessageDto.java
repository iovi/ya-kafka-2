package iovi.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Data
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class MessageDto {

    /** Текст сообщения */
    private String messageText;

    /** Ид. пользователя-отправителя */
    private Long userId;

    /** Чёрный список пользователей, которым читать это сообщение не положено */
    private List<Long> userIdsBlackList = new ArrayList<>();
}
