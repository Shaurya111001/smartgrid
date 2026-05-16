#ifndef CONSUMER_HPP
#define CONSUMER_HPP

#include "node.hpp"

class Consumer : public Node {

private:
    double max_demand;

public:
    Consumer(uint32_t node_id, uint32_t district_id, double max_demand);

    double compute_energy() override;
};

#endif