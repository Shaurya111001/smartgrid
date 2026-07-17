package com.smartgrid.presentation.controller;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smartgrid.presentation.kafka.BillingCacheService;
import com.smartgrid.presentation.kafka.NodeCacheService;
import com.smartgrid.presentation.model.NodeView;
import com.smartgrid.presentation.model.UsageRecord;

@RestController
@RequestMapping("/users")
public class PresentationController {

    private final NodeCacheService    nodeCacheService;
    private final BillingCacheService billingCacheService;

    public PresentationController(NodeCacheService nodeCacheService,
                                   BillingCacheService billingCacheService) {
        this.nodeCacheService    = nodeCacheService;
        this.billingCacheService = billingCacheService;
    }

    @GetMapping("/{id}/nodes")
    public ResponseEntity<List<NodeView>> getUserNodes(@PathVariable("id") String userId) {
        return ResponseEntity.ok(nodeCacheService.getNodesForUser(userId));
    }

    @GetMapping("/{id}/bills")
    public ResponseEntity<List<UsageRecord>> getUserBills(@PathVariable("id") String userId) {
        return ResponseEntity.ok(billingCacheService.getBillsForUser(userId));
    }
}
