#pragma once

#include <string>

class ResultWriter {
public:
    ResultWriter(const std::string& root_dir);
    ~ResultWriter();

    void record_node_delta(int step, int district_id, int node_id, double delta);
    void record_accumulator_soc(int step, int district_id, int node_id, double soc);
    void record_district_aggregate(int step, int district_id, double aggregate);

    void record_benchmark(const std::string& scenario, const std::string& strategy,
                           int processes, int rank, size_t districts_owned,
                           size_t nodes_owned, long duration_ms);

private:
    std::string root;
    void ensure_dir();
};
