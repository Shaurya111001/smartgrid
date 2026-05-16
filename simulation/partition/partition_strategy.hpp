#ifndef PARTITION_STRATEGY_HPP
#define PARTITION_STRATEGY_HPP

#include <vector>
#include "../domain/district.hpp"

class PartitionStrategy {

public:

    static std::vector<std::vector<District*>>
    district_partition(std::vector<District*>& districts, int processes);

    static std::vector<std::vector<Node*>>
    node_partition(std::vector<District*>& districts, int processes);

};

#endif