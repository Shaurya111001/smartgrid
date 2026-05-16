// Stub MPIManager implementation used when MPI isn't available on the system.
#include "mpi/mpi_manager.hpp"

MPIManager::MPIManager(int argc, char** argv) {
    (void)argc; (void)argv;
    rank = 0;
    size = 1;
}

MPIManager::~MPIManager() {}

int MPIManager::get_rank() const { return rank; }

int MPIManager::get_size() const { return size; }
