package com.smartgrid.account.controller;

import com.smartgrid.account.dto.UserEvent;
import com.smartgrid.account.kafka.UserEventProducer;
import com.smartgrid.account.model.User;
import com.smartgrid.account.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/users")
public class UserController {

    private static final Logger log = LoggerFactory.getLogger(UserController.class);

    private final UserRepository    userRepository;
    private final UserEventProducer userEventProducer;

    public UserController(UserRepository userRepository,
                          UserEventProducer userEventProducer) {
        this.userRepository    = userRepository;
        this.userEventProducer = userEventProducer;
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody User request) {
        String userId = "u_" + UUID.randomUUID().toString().substring(0, 8);
        User user = new User(userId, request.getName(), request.getEmail());

        UserEvent event = new UserEvent("UserRegistered", userId, user.getName(), user.getEmail());
        userEventProducer.publish(event);

        userRepository.save(user);

        log.info("Registered user: {}", user);
        return ResponseEntity.status(HttpStatus.CREATED).body(user);
    }

    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable("id") String id,
                                    @RequestBody User request) {
        if (!userRepository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "User not found: " + id));
        }

        User user = new User(id, request.getName(), request.getEmail());

        UserEvent event = new UserEvent("UserUpdated", id, user.getName(), user.getEmail());
        userEventProducer.publish(event);

        userRepository.save(user);

        log.info("Updated user: {}", user);
        return ResponseEntity.ok(user);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable("id") String id) {
        if (!userRepository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "User not found: " + id));
        }

        UserEvent event = new UserEvent("UserDeleted", id, null, null);
        userEventProducer.publish(event);

        userRepository.deleteById(id);

        log.info("Deleted userId={}", id);
        return ResponseEntity.ok(Map.of("message", "User deleted", "userId", id));
    }

    @GetMapping
    public ResponseEntity<Collection<User>> listAll() {
        return ResponseEntity.ok(userRepository.findAll());
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable("id") String id) {
        return userRepository.findById(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "User not found: " + id)));
    }
}
