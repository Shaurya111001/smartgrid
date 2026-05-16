package com.smartgrid.account.repository;

import com.smartgrid.account.model.User;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory user store backed by a ConcurrentHashMap.
 * Rebuilt on startup by replaying the user-events Kafka topic.
 */
@Repository
public class UserRepository {

    private final Map<String, User> store = new ConcurrentHashMap<>();

    public void save(User user) {
        store.put(user.getUserId(), user);
    }

    public Optional<User> findById(String userId) {
        return Optional.ofNullable(store.get(userId));
    }

    public void deleteById(String userId) {
        store.remove(userId);
    }

    public Collection<User> findAll() {
        return store.values();
    }

    public boolean existsById(String userId) {
        return store.containsKey(userId);
    }

    public int count() {
        return store.size();
    }

    public void clear() {
        store.clear();
    }
}
