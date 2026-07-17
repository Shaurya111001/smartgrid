#ifndef MPI_MANAGER_HPP
#define MPI_MANAGER_HPP

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

    double allreduce_sum(double local) const;
};

#endif
