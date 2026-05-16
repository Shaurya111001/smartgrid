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

private:
    std::string root;
    void ensure_dir();
};


