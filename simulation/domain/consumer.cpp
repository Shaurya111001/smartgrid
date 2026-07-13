#include "consumer.hpp"
#include <cstdlib>
#include <random>

// Fixed seed (not std::random_device) so repeated runs with the same scenario are
// reproducible -- required to fairly compare partitioning strategies against each
// other on identical underlying data, per the assignment's comparison requirement.
static std::default_random_engine generator(43);

Consumer::Consumer(uint32_t node_id, uint32_t district_id, double max_demand)
    : Node(node_id, district_id, NodeType::CONSUMER),
      max_demand(max_demand) {}

double Consumer::compute_energy() {

    std::normal_distribution<double> dist(max_demand * 0.7,
                                          max_demand * 0.2);

    double energy = dist(generator);

    if (energy < 0)
        energy = 0;

    if (energy > max_demand)
        energy = max_demand;

    // Returns a positive magnitude, not a signed delta: the wire/CSV convention
    // (matching kafka-topics.md and billing-service's Math.abs(energyValue)) is that
    // energyValue is always >= 0 and "type" alone carries direction. District::simulate_step
    // applies the sign for its own internal aggregate_balance bookkeeping.
    return energy;
}