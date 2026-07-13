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

void District::publish_measurement(int step, Node* node, double value) const {
    if (!publisher_callback) {
        return;
    }

    // Build a small JSON object by hand to avoid external dependencies.
    // timestamp: ISO-8601 UTC
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::gmtime(&t);
    std::ostringstream ts;
    ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    // Must match the canonical event shape in kafka-topics.md (lowercase
    // producer/consumer/accumulator) since analytics-service's CASE WHEN
    // matches on these exact literals when signing each measurement.
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

    // Pass 1: producers/consumers compute their own delta and contribute directly
    // to the district's raw supply/demand balance. Accumulators don't self-generate
    // (their compute_energy() is a no-op) and are handled in pass 2 below.
    for (auto node : nodes) {
        if (node->get_node_type() == NodeType::ACCUMULATOR) {
            continue;
        }

        // compute_energy() returns a magnitude (>= 0); the sign is applied here,
        // not baked into the node, so the published/recorded magnitude matches the
        // wire convention (energyValue >= 0, "type" carries direction) while the
        // district's own aggregate_balance still nets producers against consumers.
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

    // Pass 2: split the district's raw balance evenly across its accumulators
    // (applying the *full* balance to each one independently would multiply the
    // energy absorbed/released by the accumulator count, violating conservation).
    // Each accumulator's own charge is still clamped to [0, capacity] internally,
    // so the *applied* (post-clamp) delta -- not the raw share -- is what we
    // record and publish as its measurement, so downstream SOC tracking reflects
    // reality even when an accumulator is full/empty.
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

    // Record district aggregate
    if (g_result_writer) {
        g_result_writer->record_district_aggregate(step, district_id, aggregate_balance);
    }

    std::cout << "  Aggregate balance: "
              << aggregate_balance << std::endl;
}

void District::simulate_step_distributed(int step, MPIManager& mpi,
                                          const std::function<bool(Node*)>& is_owned) {

    std::cout << "District " << district_id << " (rank " << mpi.get_rank() << ")" << std::endl;

    // Pass 1: sum only the locally-owned producers/consumers into a partial total.
    // Nodes owned by other ranks contribute nothing here -- their share arrives
    // via the Allreduce below.
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

    // Collective: every rank must call this for every district, in the same order,
    // even ranks that own none of this district's nodes (local_partial=0 for them) --
    // MPI_Allreduce is a collective operation and will hang if any rank skips it.
    aggregate_balance = mpi.allreduce_sum(local_partial);

    // Every rank sees the full district structure (see main.cpp's note on redundant
    // grid generation), so counting accumulators locally gives the true global count
    // even though only locally-owned ones are updated on this rank.
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

    // Every rank computes the same reduced aggregate_balance, so only rank 0 records
    // it -- otherwise every rank would append a duplicate row for the same district/step.
    if (mpi.get_rank() == 0 && g_result_writer) {
        g_result_writer->record_district_aggregate(step, district_id, aggregate_balance);
    }

    std::cout << "  Aggregate balance: " << aggregate_balance << std::endl;
}

void District::update_accumulators() {
    // Superseded by the accumulator pass inlined in simulate_step(), which needs
    // each accumulator's applied (post-clamp) delta to record/publish correctly.
    // Kept only because it's part of the public API; not called internally anymore.
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