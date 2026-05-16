#include "district.hpp"
#include "io/result_writer.hpp"
#include "accumulator.hpp"
#include <iostream>
#include <functional>
#include <sstream>
#include <string>
#include <chrono>
#include <ctime>
#include <iomanip>

District::District(int id) {
    aggregate_balance = 0;
    district_id = id;
}

// store a pointer to a result writer (optional)
static ResultWriter* g_result_writer = nullptr;

void District::set_result_writer(ResultWriter* writer) {
    g_result_writer = writer;
}

void District::add_node(Node* node) {
    nodes.push_back(node);
}

void District::simulate_step(int step) {


    std::cout << "District " << district_id << std::endl;

     aggregate_balance = 0; 

    for (auto node : nodes) {

        double delta = node->compute_energy();

        aggregate_balance += delta;

        std::cout << "  Node " << node->get_node_id()
                  << " delta: " << delta << std::endl;

        // Record node delta
        if (g_result_writer) {
            g_result_writer->record_node_delta(step, district_id, node->get_node_id(), delta);
        }

        // If a publisher is configured, publish a measurement-event JSON for this node (include step)
        if (publisher_callback) {
                        // Build a small JSON object by hand to avoid external dependencies.
                        // timestamp: ISO-8601 UTC
                        auto now = std::chrono::system_clock::now();
                        std::time_t t = std::chrono::system_clock::to_time_t(now);
                        std::tm tm = *std::gmtime(&t);
                        std::ostringstream ts;
                        ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

                        std::string type_str = (node->get_node_type() == NodeType::PRODUCER) ? "PRODUCTION" :
                                                                     (node->get_node_type() == NodeType::CONSUMER) ? "CONSUMPTION" : "ACCUMULATOR";

                        std::ostringstream j;
                        j << "{"
                            << "\"eventType\":\"MEASUREMENT\","
                            << "\"step\":" << step << ","
                            << "\"nodeId\":" << node->get_node_id() << ","
                            << "\"districtId\":" << district_id << ","
                            << "\"type\":\"" << type_str << "\"," 
                            << "\"energyValue\":" << delta << ","
                            << "\"timestamp\":\"" << ts.str() << "\""
                            << "}";

                        std::string topic = "measurement-events";
                        std::string key = std::to_string(district_id);
                        std::string value = j.str();

                        publisher_callback(topic, key, value);
                }
    }

    update_accumulators();

    // After accumulators updated, record their SoC
    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            Accumulator* acc = dynamic_cast<Accumulator*>(node);
            if (acc != nullptr) {
                if (g_result_writer) {
                    g_result_writer->record_accumulator_soc(step, district_id, node->get_node_id(), acc->get_charge());
                }
            }
        }
    }

    // Record district aggregate
    if (g_result_writer) {
        g_result_writer->record_district_aggregate(step, district_id, aggregate_balance);
    }

    std::cout << "  Aggregate balance: "
              << aggregate_balance << std::endl;
}

void District::update_accumulators() {

    for (auto node : nodes) {

        if (node->get_node_type() == NodeType::ACCUMULATOR) {

            Accumulator* acc = dynamic_cast<Accumulator*>(node);

            if (acc != nullptr) {
                acc->update_charge(aggregate_balance);
                std::cout << "  Accumulator charge: "
                          << acc->get_charge()
                          << std::endl;
            }
        }
    }
}

double District::get_balance() const {
    return aggregate_balance;
}

std::vector<Node*>& District::get_nodes() {
    return nodes;
}