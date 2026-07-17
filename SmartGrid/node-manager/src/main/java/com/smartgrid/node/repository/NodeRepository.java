package com.smartgrid.node.repository;

import com.smartgrid.node.model.Node;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Repository
public class NodeRepository {

    private final Map<String, Node> store = new ConcurrentHashMap<>();

    public void save(Node node) {
        store.put(node.getNodeId(), node);
    }

    public Optional<Node> findById(String nodeId) {
        return Optional.ofNullable(store.get(nodeId));
    }

    public void deleteById(String nodeId) {
        store.remove(nodeId);
    }

    public Collection<Node> findAll() {
        return store.values();
    }

    public boolean existsById(String nodeId) {
        return store.containsKey(nodeId);
    }

    public int count() {
        return store.size();
    }
}
