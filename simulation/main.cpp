#include <iostream>

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

int main(int argc, char** argv) {

    MPIManager mpi_manager(argc, argv);
    int rank = mpi_manager.get_rank();
    int size = mpi_manager.get_size();

    std::cout << "Process "
              << rank
              << " of "
              << size
              << " running\n";

    Benchmark bench;
    Grid grid = ScenarioGenerator::generate_sparse_grid(2);

    // District d1(1);
    // District d2(2);

    // // Create nodes
    // Producer* p1 = new Producer(1, 1, 100);
    // Producer* p2 = new Producer(2, 1, 80);

    // Consumer* c1 = new Consumer(3, 1, 60);
    // Consumer* c2 = new Consumer(4, 1, 50);

    // Accumulator* a1 = new Accumulator(5, 1, 500, 200);

    // // Add nodes to district
    // d1.add_node(p1);
    // d1.add_node(p2);
    // d1.add_node(c1);
    // d1.add_node(c2);
    // d1.add_node(a1);

    // grid.add_district(d1);
    // grid.add_district(d2);

    bench.start();

      int steps = 10;

      auto assignments =
          PartitionStrategy::district_partition(
              grid.get_districts(),
              size
          );

      auto my_districts = assignments[rank];

      std::cout << "Rank " << rank
                << " running "
                << my_districts.size()
                << " districts\n";

    // Create a ResultWriter to record simulation outputs locally
    ResultWriter writer("results");
    for (auto d : my_districts) {
        d->set_result_writer(&writer);
    }

    // Initialize optional EventPublisher. Bootstrap servers come from env KAFKA_BOOTSTRAP_SERVERS
    std::optional<std::unique_ptr<EventPublisher>> publisher_opt;
    const char* kafka_env = std::getenv("KAFKA_BOOTSTRAP_SERVERS");
    if (kafka_env != nullptr) {
        publisher_opt = EventPublisher::create(kafka_env);
    }

    // If publisher available, set callback on districts
    if (publisher_opt) {
        EventPublisher* pub = publisher_opt->get();
        for (auto d : my_districts) {
            d->set_publisher_callback([pub](const std::string& topic, const std::string& key, const std::string& value){
                pub->publish(topic, key, value);
            });
        }
    }

      for (int step = 0; step < steps; step++) {

          for (auto d : my_districts) {
              d->simulate_step(step);
          }

      }

    bench.stop();

    bench.print_duration();

    std::cout << "Simulation finished\n";

    return 0;
}