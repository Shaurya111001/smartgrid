#include "benchmark.hpp"

void Benchmark::start() {
    start_time = std::chrono::high_resolution_clock::now();
}

void Benchmark::stop() {
    end_time = std::chrono::high_resolution_clock::now();
}

void Benchmark::print_duration() {

    auto duration =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time
        );

    std::cout << "Execution time: "
              << duration.count()
              << " ms"
              << std::endl;
}