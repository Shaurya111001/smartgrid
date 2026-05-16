#include "accumulator.hpp"

Accumulator::Accumulator(uint32_t node_id,
                         uint32_t district_id,
                         double capacity,
                         double initial_charge)
    : Node(node_id, district_id, NodeType::ACCUMULATOR),
      capacity(capacity),
      current_charge(initial_charge) {}

double Accumulator::compute_energy() {

    // Accumulators don't produce random energy
    // They respond to district balance
    return 0.0;
}

void Accumulator::update_charge(double district_balance) {

    current_charge += district_balance;

    if (current_charge > capacity)
        current_charge = capacity;

    if (current_charge < 0)
        current_charge = 0;
}

double Accumulator::get_charge() const {
    return current_charge;
}