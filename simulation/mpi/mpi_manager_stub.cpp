#include "mpi/mpi_manager.hpp"

MPIManager::MPIManager(int argc, char** argv) {
    (void)argc; (void)argv;
    rank = 0;
    size = 1;
}

MPIManager::~MPIManager() {}

int MPIManager::get_rank() const { return rank; }

int MPIManager::get_size() const { return size; }

double MPIManager::allreduce_sum(double local) const { return local; }
