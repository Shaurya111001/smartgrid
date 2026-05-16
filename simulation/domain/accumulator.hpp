#ifndef ACCUMULATOR_HPP
#define ACCUMULATOR_HPP

#include "node.hpp"

class Accumulator : public Node {

private:
    double capacity;
    double current_charge;

public:
    Accumulator(uint32_t node_id,
                uint32_t district_id,
                double capacity,
                double initial_charge);

    double compute_energy() override;

    void update_charge(double district_balance);

    double get_charge() const;
};

#endif