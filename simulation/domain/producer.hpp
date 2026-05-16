#ifndef PRODUCER_HPP
#define PRODUCER_HPP

#include "node.hpp"

class Producer : public Node {

private:
    double max_generation;

public:
    Producer(uint32_t node_id, uint32_t district_id, double max_generation);

    double compute_energy() override;
};

#endif