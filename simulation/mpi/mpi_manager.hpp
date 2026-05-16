#ifndef MPI_MANAGER_HPP
#define MPI_MANAGER_HPP

// Include MPI header if available; if not, this header remains compilable and
// a stub implementation is used (mpi_manager_stub.cpp).
#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

class MPIManager {

private:
    int rank;
    int size;

public:

    MPIManager(int argc, char** argv);

    ~MPIManager();

    int get_rank() const;

    int get_size() const;
};

#endif

