#pragma once

#include <string>
#include <memory>
#include <optional>

class EventPublisher {
public:
    static std::optional<std::unique_ptr<EventPublisher>> create(const std::string& bootstrap);

    ~EventPublisher();

    bool publish(const std::string& topic, const std::string& key, const std::string& value);

private:
    EventPublisher();
    struct Impl;
    Impl* impl;
};
