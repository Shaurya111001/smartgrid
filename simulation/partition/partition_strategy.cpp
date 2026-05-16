#include "partition_strategy.hpp"

std::vector<std::vector<District*>>
PartitionStrategy::district_partition(std::vector<District*>& districts,
                                      int processes)
{

    std::vector<std::vector<District*>> assignments(processes);

    for (size_t i = 0; i < districts.size(); i++) {

        int process = i % processes;

        assignments[process].push_back(districts[i]);
    }

    return assignments;
}

std::vector<std::vector<Node*>>
PartitionStrategy::node_partition(std::vector<District*>& districts,
                                 int processes)
{

    std::vector<std::vector<Node*>> assignments(processes);

    int process = 0;

    for (auto district : districts) {

        for (auto node : district->get_nodes()) {

            assignments[process].push_back(node);

            process = (process + 1) % processes;
        }
    }

    return assignments;
}