#include "benchmark.hpp"

void Benchmark::start() {
    start_time = std::chrono::high_resolution_clock::now();
}

void Benchmark::stop() {
    end_time = std::chrono::high_resolution_clock::now();
}

void Benchmark::print_duration() {
    std::cout << "Execution time: "
              << duration_ms()
              << " ms"
              << std::endl;
}

long Benchmark::duration_ms() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
            end_time - start_time
        ).count();
}
