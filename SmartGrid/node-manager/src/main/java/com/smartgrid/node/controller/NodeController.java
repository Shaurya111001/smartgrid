package com.smartgrid.node.controller;

import com.smartgrid.node.dto.NodeEvent;
import com.smartgrid.node.kafka.NodeEventProducer;
import com.smartgrid.node.kafka.UserCacheService;
import com.smartgrid.node.model.Node;
import com.smartgrid.node.repository.NodeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/nodes")
public class NodeController {

    private static final Logger log = LoggerFactory.getLogger(NodeController.class);

    private final NodeRepository    nodeRepository;
    private final NodeEventProducer nodeEventProducer;
    private final UserCacheService  userCacheService;

    public NodeController(NodeRepository nodeRepository,
                          NodeEventProducer nodeEventProducer,
                          UserCacheService userCacheService) {
        this.nodeRepository    = nodeRepository;
        this.nodeEventProducer = nodeEventProducer;
        this.userCacheService  = userCacheService;
    }

    // ──── POST /nodes ─────────────────────────────────────────────────
    @PostMapping
    public ResponseEntity<?> create(@RequestBody Node request) {

        // Validate that the user exists
        if (!userCacheService.userExists(request.getUserId())) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "User not found: " + request.getUserId()));
        }

        String nodeId = "n_" + UUID.randomUUID().toString().substring(0, 8);
        Node node = new Node(nodeId, request.getUserId(), request.getDistrictId(), request.getType());

        // Publish first: only commit to the local store once Kafka has actually
        // acknowledged the event, so the two can never diverge on a publish failure.
        NodeEvent event = new NodeEvent("NodeCreated", nodeId,
                node.getUserId(), node.getDistrictId(), node.getType());
        nodeEventProducer.publish(event);

        nodeRepository.save(node);

        log.info("Created node: {}", node);
        return ResponseEntity.status(HttpStatus.CREATED).body(node);
    }

    // ──── PUT /nodes/{id} ─────────────────────────────────────────────
    @PutMapping("/{id}")
    public ResponseEntity<?> update(@PathVariable("id") String id,
                                    @RequestBody Node request) {
        if (!nodeRepository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Node not found: " + id));
        }

        Node node = new Node(id, request.getUserId(), request.getDistrictId(), request.getType());

        NodeEvent event = new NodeEvent("NodeUpdated", id,
                node.getUserId(), node.getDistrictId(), node.getType());
        nodeEventProducer.publish(event);

        nodeRepository.save(node);

        log.info("Updated node: {}", node);
        return ResponseEntity.ok(node);
    }

    // ──── DELETE /nodes/{id} ──────────────────────────────────────────
    @DeleteMapping("/{id}")
    public ResponseEntity<?> delete(@PathVariable("id") String id) {
        if (!nodeRepository.existsById(id)) {
            return ResponseEntity.status(HttpStatus.NOT_FOUND)
                    .body(Map.of("error", "Node not found: " + id));
        }

        NodeEvent event = new NodeEvent("NodeDeleted", id, null, null, null);
        nodeEventProducer.publish(event);

        nodeRepository.deleteById(id);

        log.info("Deleted nodeId={}", id);
        return ResponseEntity.ok(Map.of("message", "Node deleted", "nodeId", id));
    }

    // ──── GET /nodes (convenience) ────────────────────────────────────
    @GetMapping
    public ResponseEntity<Collection<Node>> listAll() {
        return ResponseEntity.ok(nodeRepository.findAll());
    }

    // ──── GET /nodes/{id} (convenience) ───────────────────────────────
    @GetMapping("/{id}")
    public ResponseEntity<?> getById(@PathVariable("id") String id) {
        return nodeRepository.findById(id)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElse(ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(Map.of("error", "Node not found: " + id)));
    }
}
