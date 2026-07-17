#include <iostream>
#include <string>
#include <unordered_set>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <sys/resource.h>

#include "domain/grid.hpp"
#include "domain/district.hpp"
#include "domain/producer.hpp"
#include "domain/consumer.hpp"
#include "domain/accumulator.hpp"
#include "benchmark/benchmark.hpp"
#include "scenario/scenario_generator.hpp"
#include "mpi/mpi_manager.hpp"
#include "partition/partition_strategy.hpp"
#include "kafka/event_publisher.hpp"
#include "io/result_writer.hpp"

namespace {

// Total CPU time (user+system) consumed by this process so far. Diffing two
// calls across an interval and comparing against that interval's wall-clock
// time gives a CPU utilization percentage for that interval.
double cpu_seconds() {
    struct rusage usage;
    getrusage(RUSAGE_SELF, &usage);
    return (usage.ru_utime.tv_sec + usage.ru_utime.tv_usec / 1e6) +
           (usage.ru_stime.tv_sec + usage.ru_stime.tv_usec / 1e6);
}

std::string iso_timestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t t = std::chrono::system_clock::to_time_t(now);
    std::tm tm = *std::gmtime(&t);
    std::ostringstream ts;
    ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return ts.str();
}

}  // namespace

// Usage: simulation_app [sparse|dense] [roundrobin|weighted|nodepartition] [district_count]
//
// Exposed as CLI args (rather than hardcoded) specifically so the assignment's
// required comparison -- dense vs. sparse districts, task-allocation strategy vs.
// strategy, across computation time / communication overhead / scalability -- can
// actually be run: e.g.
//   mpirun -np 4 ./simulation_app dense weighted 8
//   mpirun -np 4 ./simulation_app dense roundrobin 8
//   mpirun -np 4 ./simulation_app dense nodepartition 8
// and diff the resulting results/<scenario>_<strategy>_p<N>/benchmark.csv files.
//
// "roundrobin" and "weighted" assign whole districts to ranks -- zero inter-rank
// communication, but a district's node count is a lower bound on how finely work
// can be spread. "nodepartition" splits individual nodes across ranks instead, so
// finer-grained load balancing is possible, at the cost of a real MPI_Allreduce
// per district per step (see District::simulate_step_distributed) -- this is the
// strategy to use to get non-zero, meaningful communication-overhead numbers for
// the comparison the assignment asks for.
int main(int argc, char** argv) {

    MPIManager mpi_manager(argc, argv);
    int rank = mpi_manager.get_rank();
    int size = mpi_manager.get_size();

    std::string scenario = (argc > 1) ? argv[1] : "sparse";
    std::string strategy = (argc > 2) ? argv[2] : "roundrobin";
    int district_count   = (argc > 3) ? std::stoi(argv[3]) : 4;

    std::cout << "Process " << rank << " of " << size
              << " running (scenario=" << scenario
              << ", strategy=" << strategy
              << ", districts=" << district_count << ")\n";

    Benchmark bench;
    bench.start();

    // Every rank independently (and redundantly) builds the full grid from the
    // same fixed RNG seeds (see producer.cpp/consumer.cpp), so all ranks agree on
    // structure and values without needing to transmit the grid itself.
    Grid grid = (scenario == "dense")
        ? ScenarioGenerator::generate_dense_grid(district_count)
        : ScenarioGenerator::generate_sparse_grid(district_count);

    int steps = 10;

    // Tag the results directory with the run configuration so multiple runs
    // (different scenario/strategy/process-count combinations) don't clobber
    // each other and can be compared side by side afterward.
    std::string results_dir = "results/" + scenario + "_" + strategy + "_p" + std::to_string(size);
    ResultWriter writer(results_dir);

    // Initialize optional EventPublisher. Bootstrap servers come from env KAFKA_BOOTSTRAP_SERVERS
    std::optional<std::unique_ptr<EventPublisher>> publisher_opt;
    const char* kafka_env = std::getenv("KAFKA_BOOTSTRAP_SERVERS");
    if (kafka_env != nullptr) {
        publisher_opt = EventPublisher::create(kafka_env);
    }
    EventPublisher* pub = publisher_opt ? publisher_opt->get() : nullptr;
    auto publish_cb = [pub](const std::string& topic, const std::string& key, const std::string& value) {
        if (pub) pub->publish(topic, key, value);
    };

    // Correlates every metric event from every rank of this run so a caller
    // (e.g. simulation-service, which sets this env var when it launches
    // mpirun) can query "just this run's" metrics. Falls back to a
    // locally-generated id so manual/local invocations (e.g.
    // test_kafka_spark_mpi.sh) keep working unchanged.
    const char* run_id_env = std::getenv("SIMULATION_RUN_ID");
    std::string run_id = (run_id_env != nullptr)
        ? run_id_env
        : ("local-" + std::to_string(std::chrono::system_clock::now().time_since_epoch().count()));

    // Resource-usage telemetry for this run, published per rank per step --
    // this is deliberately separate from publish_measurement()/"measurement-events"
    // (grid energy readings): this topic reports how the *simulation itself* is
    // using compute resources (wall-clock, CPU%), not simulated sensor data.
    auto publish_step_metrics = [&](int step, double wall_clock_ms, double cpu_percent) {
        if (!pub) return;
        std::ostringstream j;
        j << "{"
            << "\"runId\":\"" << run_id << "\","
            << "\"rank\":" << rank << ","
            << "\"processes\":" << size << ","
            << "\"scenario\":\"" << scenario << "\","
            << "\"strategy\":\"" << strategy << "\","
            << "\"step\":" << step << ","
            << "\"stepWallClockMs\":" << wall_clock_ms << ","
            << "\"cpuUtilizationPercent\":" << cpu_percent << ","
            << "\"timestamp\":\"" << iso_timestamp() << "\""
            << "}";
        pub->publish("simulation-metrics", run_id, j.str());
    };

    size_t districts_owned = 0;
    size_t nodes_owned = 0;

    if (strategy == "nodepartition") {
        // Node-level partitioning: every rank sees every district (so it can
        // participate in each district's collective Allreduce), but only computes
        // and publishes the individual nodes it owns.
        for (auto d : grid.get_districts()) {
            d->set_result_writer(&writer);
            if (pub) d->set_publisher_callback(publish_cb);
        }

        auto my_nodes = PartitionStrategy::node_partition(grid.get_districts(), size);
        std::unordered_set<Node*> owned(my_nodes[rank].begin(), my_nodes[rank].end());
        nodes_owned = owned.size();
        districts_owned = grid.get_districts().size(); // every rank touches every district

        std::cout << "Rank " << rank << " owns " << nodes_owned
                  << " nodes across " << districts_owned << " districts\n";

        auto is_owned = [&owned](Node* n) { return owned.count(n) > 0; };

        for (int step = 0; step < steps; step++) {
            auto step_start = std::chrono::high_resolution_clock::now();
            double cpu_start = cpu_seconds();

            for (auto d : grid.get_districts()) {
                d->simulate_step_distributed(step, mpi_manager, is_owned);
            }

            double wall_clock_ms = std::chrono::duration<double, std::milli>(
                std::chrono::high_resolution_clock::now() - step_start).count();
            double cpu_percent = wall_clock_ms > 0
                ? 100.0 * (cpu_seconds() - cpu_start) * 1000.0 / wall_clock_ms
                : 0.0;
            publish_step_metrics(step, wall_clock_ms, cpu_percent);
        }
    } else {
        // District-level partitioning (roundrobin or weighted): whole districts are
        // assigned to ranks, so each rank only ever touches its own subset and needs
        // no communication with the others.
        auto assignments = (strategy == "weighted")
            ? PartitionStrategy::weighted_district_partition(grid.get_districts(), size)
            : PartitionStrategy::district_partition(grid.get_districts(), size);

        auto my_districts = assignments[rank];
        districts_owned = my_districts.size();
        for (auto d : my_districts) {
            nodes_owned += d->get_nodes().size();
        }

        std::cout << "Rank " << rank
                  << " running " << districts_owned
                  << " districts (" << nodes_owned << " nodes)\n";

        for (auto d : my_districts) {
            d->set_result_writer(&writer);
            if (pub) d->set_publisher_callback(publish_cb);
        }

        for (int step = 0; step < steps; step++) {
            auto step_start = std::chrono::high_resolution_clock::now();
            double cpu_start = cpu_seconds();

            for (auto d : my_districts) {
                d->simulate_step(step);
            }

            double wall_clock_ms = std::chrono::duration<double, std::milli>(
                std::chrono::high_resolution_clock::now() - step_start).count();
            double cpu_percent = wall_clock_ms > 0
                ? 100.0 * (cpu_seconds() - cpu_start) * 1000.0 / wall_clock_ms
                : 0.0;
            publish_step_metrics(step, wall_clock_ms, cpu_percent);
        }
    }

    bench.stop();
    bench.print_duration();

    writer.record_benchmark(scenario, strategy, size, rank,
                             districts_owned, nodes_owned, bench.duration_ms());

    std::cout << "Simulation finished\n";

    return 0;
}
