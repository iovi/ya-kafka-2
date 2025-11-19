package iovi.service;

import iovi.dto.User;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class UserService {

    @Value("${my.users.number}")
    private int usersNumber;

    @Getter
    private List<User> users;

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
}
