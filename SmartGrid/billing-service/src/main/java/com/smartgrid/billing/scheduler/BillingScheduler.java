package com.smartgrid.billing.scheduler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.billing.dto.UsageRecordEvent;
import com.smartgrid.billing.kafka.MeasurementReplayService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Every 60 seconds, drains accumulated usage and publishes
 * a UsageRecordCreated event per user to the billing-events topic.
 */
@Component
public class BillingScheduler {

    private static final Logger log = LoggerFactory.getLogger(BillingScheduler.class);
    private static final String TOPIC = "billing-events";
    private static final double RATE_PER_KWH = 0.20; // €/kWh

    private final MeasurementReplayService replayService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private Instant lastBillingTime = Instant.now();

    public BillingScheduler(MeasurementReplayService replayService,
                            KafkaTemplate<String, String> kafkaTemplate,
                            ObjectMapper objectMapper) {
        this.replayService = replayService;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    @Scheduled(fixedRate = 60_000) // every 60 seconds
    public void generateBillingRecords() {
        Instant now = Instant.now();
        Instant periodStart = lastBillingTime;
        lastBillingTime = now;

        Map<String, Double> usage = replayService.drainUsage();

        if (usage.isEmpty()) {
            log.debug("No usage to bill this period");
            return;
        }

        log.info("💰 Billing cycle: {} user(s) with usage", usage.size());

        for (Map.Entry<String, Double> entry : usage.entrySet()) {
            String userId       = entry.getKey();
            double totalKwh     = Math.round(entry.getValue() * 100.0) / 100.0;
            double cost         = Math.round(totalKwh * RATE_PER_KWH * 100.0) / 100.0;

            UsageRecordEvent event = new UsageRecordEvent(
                    userId, totalKwh, cost, periodStart, now);

            try {
                String json = objectMapper.writeValueAsString(event);
                SendResult<String, String> result =
                        kafkaTemplate.send(TOPIC, userId, json).get(5, TimeUnit.SECONDS);
                var meta = result.getRecordMetadata();
                log.info("Published UsageRecordCreated: userId={}, kwh={}, cost=€{} -> {}-{}@{}",
                         userId, totalKwh, cost, meta.topic(), meta.partition(), meta.offset());
            } catch (JsonProcessingException e) {
                log.error("Failed to serialize billing event for userId={}; usage restored for next cycle", userId, e);
                replayService.restoreUsage(userId, totalKwh);
            } catch (ExecutionException e) {
                log.error("Kafka rejected billing event for userId={}; usage restored for next cycle: {}",
                        userId, e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
                replayService.restoreUsage(userId, totalKwh);
            } catch (TimeoutException e) {
                log.error("Timed out publishing billing event for userId={}; usage restored for next cycle", userId);
                replayService.restoreUsage(userId, totalKwh);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Interrupted publishing billing event for userId={}; usage restored for next cycle", userId);
                replayService.restoreUsage(userId, totalKwh);
            }
        }
    }
}
