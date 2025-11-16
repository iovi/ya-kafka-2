package iovi.service;

import iovi.dto.User;
import iovi.util.RandomMessageUtilService;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class Producer {

    private List<User> users;

    @Value("${my.users.number}")
    private int usersNumber;

    @PostConstruct
    public void setUpUsers(){
        users = new ArrayList<>();
        for(long i = 1L; i<=usersNumber; i++) {
            User user = new User();
            user.setId(i);
            user.setName(String.format("Vasya%d", i));
            users.add(user);
        }
    }

    @Scheduled(fixedDelay = 2000)
    public void sendMessages(){
        users.forEach(u-> log.info("send from {} : {}", u, RandomMessageUtilService.getRandomWord()));
    }

}
