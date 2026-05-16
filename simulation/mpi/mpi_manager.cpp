#include "mpi_manager.hpp"

MPIManager::MPIManager(int argc, char** argv) {

    MPI_Init(&argc, &argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
}

MPIManager::~MPIManager() {
    MPI_Finalize();
}

int MPIManager::get_rank() const {
    return rank;
}

int MPIManager::get_size() const {
    return size;
}