package iovi.job;

import iovi.dto.MessageDto;
import iovi.service.UserService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class Consumer {

    private final UserService userService;

    @KafkaListener(topics = "${my.kafka.out.topic}", groupId = "group1")
    public void listen(MessageDto message) {
        //продемонстрируем, что будет получать каждый из пользвоателей
        userService.getUsers().forEach( u-> {
            //пользователь не должен использовать сообщение, у которого он указан в чёрном списке
            if (!message.getUserIdsBlackList().contains(u.getId())
                    //и не должен получать сообщение сам от себя
                    && !u.getId().equals(message.getUserId())) {
                log.info("user {} consumed: {}", u.getId(), message);
            }
        });
    }
}
