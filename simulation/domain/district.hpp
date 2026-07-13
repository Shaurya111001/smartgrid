#ifndef DISTRICT_HPP
#define DISTRICT_HPP

#include <vector>
#include "node.hpp"
#include "mpi/mpi_manager.hpp"
#include <functional>
#include <string>

class District {

private:
    std::vector<Node*> nodes;
    double aggregate_balance;
    int district_id;
    // Optional publisher callback: topic, key, value
    std::function<void(const std::string&, const std::string&, const std::string&)> publisher_callback;

public:
    District(int id);

    void add_node(Node* node);

    void set_publisher_callback(std::function<void(const std::string&, const std::string&, const std::string&)> cb) {
        publisher_callback = cb;
    }

    void set_result_writer(class ResultWriter* writer);

    void simulate_step(int step);

    // Used when nodes (not whole districts) are partitioned across ranks: every
    // rank calls this for every district (is_owned tells it which of that
    // district's nodes are locally owned), and the true district-wide balance is
    // obtained via a collective MPI_Allreduce across all ranks -- the actual
    // communication cost that this partitioning mode incurs and district-level
    // partitioning doesn't.
    void simulate_step_distributed(int step, MPIManager& mpi,
                                    const std::function<bool(Node*)>& is_owned);

    void update_accumulators();

    double get_balance() const;

    std::vector<Node*>& get_nodes();

private:
    void publish_measurement(int step, Node* node, double value) const;
};

#endif