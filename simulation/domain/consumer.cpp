#include "consumer.hpp"
#include <cstdlib>
#include <random>

static std::default_random_engine generator(std::random_device{}());

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

    return -energy;
}