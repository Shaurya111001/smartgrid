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

static ResultWriter* g_result_writer = nullptr;

void District::set_result_writer(ResultWriter* writer) {
    g_result_writer = writer;
}

void District::add_node(Node* node) {
    nodes.push_back(node);
}

void District::publish_measurement(int step, Node* node, double value) const {
    if (!publisher_callback) {
        return;
    }

    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::gmtime(&t);
    std::ostringstream ts;
    ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    std::string type_str = (node->get_node_type() == NodeType::PRODUCER) ? "producer" :
                            (node->get_node_type() == NodeType::CONSUMER) ? "consumer" : "accumulator";

    std::ostringstream j;
    j << "{"
        << "\"eventType\":\"MEASUREMENT\","
        << "\"step\":" << step << ","
        << "\"nodeId\":\"" << node->get_node_id() << "\","
        << "\"districtId\":\"" << district_id << "\","
        << "\"type\":\"" << type_str << "\","
        << "\"energyValue\":" << value << ","
        << "\"timestamp\":\"" << ts.str() << "\""
        << "}";

    publisher_callback("measurement-events", std::to_string(district_id), j.str());
}

void District::simulate_step(int step) {

    std::cout << "District " << district_id << std::endl;

    aggregate_balance = 0;

    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            continue;
        }

        double magnitude = node->compute_energy();
        double signed_delta = (node->get_node_type() == NodeType::CONSUMER) ? -magnitude : magnitude;
        aggregate_balance += signed_delta;

        std::cout << "  Node " << node->get_node_id()
                  << " delta: " << signed_delta << std::endl;

        if (g_result_writer) {
            g_result_writer->record_node_delta(step, district_id, node->get_node_id(), signed_delta);
        }

        publish_measurement(step, node, magnitude);
    }

    int accumulator_count = 0;
    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            accumulator_count++;
        }
    }
    double share = (accumulator_count > 0) ? (aggregate_balance / accumulator_count) : 0.0;

    for (auto node : nodes) {
        if (node->get_node_type() != NodeType::ACCUMULATOR) {
            continue;
        }

        Accumulator* acc = dynamic_cast<Accumulator*>(node);
        if (acc == nullptr) {
            continue;
        }

        double applied_delta = acc->update_charge(share);

        std::cout << "  Accumulator charge: " << acc->get_charge() << std::endl;

        if (g_result_writer) {
            g_result_writer->record_accumulator_soc(step, district_id, node->get_node_id(), acc->get_charge());
        }

        publish_measurement(step, node, applied_delta);
    }

    if (g_result_writer) {
        g_result_writer->record_district_aggregate(step, district_id, aggregate_balance);
    }

    std::cout << "  Aggregate balance: "
              << aggregate_balance << std::endl;
}

void District::simulate_step_distributed(int step, MPIManager& mpi,
                                          const std::function<bool(Node*)>& is_owned) {

    std::cout << "District " << district_id << " (rank " << mpi.get_rank() << ")" << std::endl;

    double local_partial = 0.0;

    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR || !is_owned(node)) {
            continue;
        }

        double magnitude = node->compute_energy();
        double signed_delta = (node->get_node_type() == NodeType::CONSUMER) ? -magnitude : magnitude;
        local_partial += signed_delta;

        std::cout << "  [owned] Node " << node->get_node_id()
                  << " delta: " << signed_delta << std::endl;

        if (g_result_writer) {
            g_result_writer->record_node_delta(step, district_id, node->get_node_id(), signed_delta);
        }

        publish_measurement(step, node, magnitude);
    }

    aggregate_balance = mpi.allreduce_sum(local_partial);

    int accumulator_count = 0;
    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            accumulator_count++;
        }
    }
    double share = (accumulator_count > 0) ? (aggregate_balance / accumulator_count) : 0.0;

    for (auto node : nodes) {
        if (node->get_node_type() != NodeType::ACCUMULATOR || !is_owned(node)) {
            continue;
        }

        Accumulator* acc = dynamic_cast<Accumulator*>(node);
        if (acc == nullptr) {
            continue;
        }

        double applied_delta = acc->update_charge(share);

        std::cout << "  [owned] Accumulator charge: " << acc->get_charge() << std::endl;

        if (g_result_writer) {
            g_result_writer->record_accumulator_soc(step, district_id, node->get_node_id(), acc->get_charge());
        }

        publish_measurement(step, node, applied_delta);
    }

    if (mpi.get_rank() == 0 && g_result_writer) {
        g_result_writer->record_district_aggregate(step, district_id, aggregate_balance);
    }

    std::cout << "  Aggregate balance: " << aggregate_balance << std::endl;
}

void District::update_accumulators() {
    int accumulator_count = 0;
    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            accumulator_count++;
        }
    }
    double share = (accumulator_count > 0) ? (aggregate_balance / accumulator_count) : 0.0;

    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            Accumulator* acc = dynamic_cast<Accumulator*>(node);
            if (acc != nullptr) {
                acc->update_charge(share);
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
