#ifndef BENCHMARK_HPP
#define BENCHMARK_HPP

#include <chrono>
#include <iostream>

class Benchmark {

private:
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point end_time;

public:

    void start();

    void stop();

    void print_duration();

    long duration_ms() const;
};

#endif