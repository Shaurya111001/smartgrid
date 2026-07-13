#pragma once

#include <string>

class ResultWriter {
public:
    // root_dir: folder where results will be written (created if missing)
    ResultWriter(const std::string& root_dir);
    ~ResultWriter();

    void record_node_delta(int step, int district_id, int node_id, double delta);
    void record_accumulator_soc(int step, int district_id, int node_id, double soc);
    void record_district_aggregate(int step, int district_id, double aggregate);

    // Appends one row per rank per run: (scenario, strategy, processes, rank,
    // districts_owned, nodes_owned, duration_ms) so strategy/scenario comparisons
    // (per the assignment's required dense-vs-sparse, strategy-vs-strategy analysis)
    // can be assembled from results/benchmark.csv across multiple invocations.
    void record_benchmark(const std::string& scenario, const std::string& strategy,
                           int processes, int rank, size_t districts_owned,
                           size_t nodes_owned, long duration_ms);

private:
    std::string root;
    void ensure_dir();
};


