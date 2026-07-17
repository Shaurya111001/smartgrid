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

double MPIManager::allreduce_sum(double local) const {
    double global = 0.0;
    MPI_Allreduce(&local, &global, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
    return global;
}
