#ifndef PARTITION_STRATEGY_HPP
#define PARTITION_STRATEGY_HPP

#include <vector>
#include "../domain/district.hpp"

class PartitionStrategy {

public:

    static std::vector<std::vector<District*>>
    district_partition(std::vector<District*>& districts, int processes);

    // Greedy load-balanced assignment: districts are sorted by node count
    // (descending) and each is assigned to the currently least-loaded process.
    // Unlike district_partition's naive round-robin (which assumes districts are
    // roughly equal-sized), this stays balanced when district sizes vary a lot --
    // e.g. a mix of dense and sparse districts in the same run -- at the cost of a
    // one-time O(n log n) sort during setup.
    static std::vector<std::vector<District*>>
    weighted_district_partition(std::vector<District*>& districts, int processes);

    static std::vector<std::vector<Node*>>
    node_partition(std::vector<District*>& districts, int processes);

};

#endif