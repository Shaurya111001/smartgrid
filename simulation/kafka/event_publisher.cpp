#include "kafka/event_publisher.hpp"

#include <iostream>
#include <cstdlib>

#ifdef RDKAFKA_AVAILABLE
#include <rdkafka.h>
#endif

// No external JSON dependency: simulation will emit plain JSON strings built locally.

struct EventPublisher::Impl {
#ifdef RDKAFKA_LIB
    rd_kafka_t* rk = nullptr;
    rd_kafka_conf_t* conf = nullptr;
#endif
};

EventPublisher::EventPublisher() : impl(new Impl()) {}

EventPublisher::~EventPublisher() {
#ifdef RDKAFKA_LIB
    if (impl->rk) {
        rd_kafka_flush(impl->rk, 5000);
        rd_kafka_destroy(impl->rk);
    }
    if (impl->conf) {
        rd_kafka_conf_destroy(impl->conf);
    }
#endif
    delete impl;
}

std::optional<std::unique_ptr<EventPublisher>> EventPublisher::create(const std::string& bootstrap) {
#ifdef RDKAFKA_LIB
    auto pub = std::unique_ptr<EventPublisher>(new EventPublisher());
    pub->impl->conf = rd_kafka_conf_new();

    char errstr[512];
    if (rd_kafka_conf_set(pub->impl->conf, "bootstrap.servers", bootstrap.c_str(), errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
        std::cerr << "rdkafka conf set failed: " << errstr << std::endl;
        return std::nullopt;
    }

    pub->impl->rk = rd_kafka_new(RD_KAFKA_PRODUCER, pub->impl->conf, errstr, sizeof(errstr));
    if (!pub->impl->rk) {
        std::cerr << "Failed to create rdkafka producer: " << errstr << std::endl;
        return std::nullopt;
    }

    return std::optional<std::unique_ptr<EventPublisher>>(std::move(pub));
#else
    (void)bootstrap;
    std::cerr << "librdkafka not available at compile time; EventPublisher disabled." << std::endl;
    return std::nullopt;
#endif
}

bool EventPublisher::publish(const std::string& topic, const std::string& key, const std::string& value) {
#ifdef RDKAFKA_LIB
    rd_kafka_resp_err_t err;

    rd_kafka_topic_t* rkt = rd_kafka_topic_new(impl->rk, topic.c_str(), nullptr);
    if (!rkt) {
        std::cerr << "Failed to create topic handle" << std::endl;
        return false;
    }

    err = rd_kafka_produce(
        rkt,
        RD_KAFKA_PARTITION_UA,
        RD_KAFKA_MSG_F_COPY,
        const_cast<char*>(value.data()), value.size(),
        key.data(), key.size(),
        nullptr
    );

    rd_kafka_topic_destroy(rkt);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR) {
        std::cerr << "Failed to produce: " << rd_kafka_err2str(err) << std::endl;
        return false;
    }

    // Let librdkafka handle delivery in background; optionally flush soon
    rd_kafka_poll(impl->rk, 0);
    return true;
#else
    (void)topic; (void)key; (void)value;
    return false;
#endif
}
