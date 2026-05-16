#include "producer.hpp"
#include <cstdlib>
#include <random>

static std::default_random_engine generator(std::random_device{}());

Producer::Producer(uint32_t node_id, uint32_t district_id, double max_generation)
    : Node(node_id, district_id, NodeType::PRODUCER),
      max_generation(max_generation) {}

double Producer::compute_energy() {
    
    std::normal_distribution<double> dist(max_generation * 0.7,
                                          max_generation * 0.2);
    double energy = dist(generator);
    if (energy < 0)
        energy = 0;

    if (energy > max_generation)
        energy = max_generation;

    return energy;
}