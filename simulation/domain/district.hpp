#ifndef DISTRICT_HPP
#define DISTRICT_HPP

#include <vector>
#include "node.hpp"
#include <functional>
#include <string>

class District {

private:
    std::vector<Node*> nodes;
    double aggregate_balance;
    int district_id;
    // Optional publisher callback: topic, key, value
    std::function<void(const std::string&, const std::string&, const std::string&)> publisher_callback;

public:
    District(int id);

    void add_node(Node* node);

    void set_publisher_callback(std::function<void(const std::string&, const std::string&, const std::string&)> cb) {
        publisher_callback = cb;
    }

    void set_result_writer(class ResultWriter* writer);

    void simulate_step(int step);

    void update_accumulators();

    double get_balance() const;

    std::vector<Node*>& get_nodes();
};

#endif