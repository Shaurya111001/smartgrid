#pragma once

#include <string>
#include <memory>
#include <optional>

// Lightweight wrapper around librdkafka producer. Compilation guarded by CMake option.
class EventPublisher {
public:
    // Create publisher with bootstrap servers (e.g. "kafka:29092")
    static std::optional<std::unique_ptr<EventPublisher>> create(const std::string& bootstrap);

    ~EventPublisher();

    // publish a message to topic, with key and value (both strings)
    bool publish(const std::string& topic, const std::string& key, const std::string& value);

private:
    EventPublisher();
    struct Impl;
    Impl* impl;
};
