#include "io/result_writer.hpp"
#include <fstream>
#include <filesystem>
#include <iostream>

ResultWriter::ResultWriter(const std::string& root_dir) : root(root_dir) {
    ensure_dir();
}

ResultWriter::~ResultWriter() {}

void ResultWriter::ensure_dir() {
    try {
        std::filesystem::create_directories(root);
    } catch (...) {
        std::cerr << "Warning: cannot create results dir: " << root << std::endl;
    }
}

void ResultWriter::record_node_delta(int step, int district_id, int node_id, double delta) {
    std::ofstream f(root + "/node_deltas.csv", std::ios::app);
    f << step << "," << district_id << "," << node_id << "," << delta << "\n";
}

void ResultWriter::record_accumulator_soc(int step, int district_id, int node_id, double soc) {
    std::ofstream f(root + "/accumulator_soc.csv", std::ios::app);
    f << step << "," << district_id << "," << node_id << "," << soc << "\n";
}

void ResultWriter::record_district_aggregate(int step, int district_id, double aggregate) {
    std::ofstream f(root + "/district_aggregate.csv", std::ios::app);
    f << step << "," << district_id << "," << aggregate << "\n";
}

void ResultWriter::record_benchmark(const std::string& scenario, const std::string& strategy,
                                     int processes, int rank, size_t districts_owned,
                                     size_t nodes_owned, long duration_ms) {
    std::ofstream f(root + "/benchmark.csv", std::ios::app);
    f << scenario << "," << strategy << "," << processes << "," << rank << ","
      << districts_owned << "," << nodes_owned << "," << duration_ms << "\n";
}
