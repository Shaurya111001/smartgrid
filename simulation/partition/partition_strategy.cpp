#include "partition_strategy.hpp"
#include <algorithm>

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

std::vector<std::vector<District*>>
PartitionStrategy::weighted_district_partition(std::vector<District*>& districts,
                                               int processes)
{
    std::vector<District*> sorted = districts;
    std::sort(sorted.begin(), sorted.end(), [](District* a, District* b) {
        return a->get_nodes().size() > b->get_nodes().size();
    });

    std::vector<std::vector<District*>> assignments(processes);
    std::vector<size_t> load(processes, 0);

    for (District* d : sorted) {
        int lightest = 0;
        for (int p = 1; p < processes; p++) {
            if (load[p] < load[lightest]) {
                lightest = p;
            }
        }
        assignments[lightest].push_back(d);
        load[lightest] += d->get_nodes().size();
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
