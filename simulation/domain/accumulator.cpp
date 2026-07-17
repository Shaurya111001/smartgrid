#include "accumulator.hpp"

Accumulator::Accumulator(uint32_t node_id,
                         uint32_t district_id,
                         double capacity,
                         double initial_charge)
    : Node(node_id, district_id, NodeType::ACCUMULATOR),
      capacity(capacity),
      current_charge(initial_charge) {}

double Accumulator::compute_energy() {

    return 0.0;
}

double Accumulator::update_charge(double district_balance) {

    double before = current_charge;
    current_charge += district_balance;

    if (current_charge > capacity)
        current_charge = capacity;

    if (current_charge < 0)
        current_charge = 0;

    return current_charge - before;
}

double Accumulator::get_charge() const {
    return current_charge;
}
