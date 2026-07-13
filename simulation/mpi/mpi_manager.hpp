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

    /** Sums `local` across every rank and returns the result to all of them
     *  (MPI_Allreduce/MPI_SUM). This is the actual point-to-point communication
     *  cost that node_partition incurs and district_partition doesn't -- the
     *  reason the two strategies can be compared on "communication overhead" at
     *  all. The stub (no-MPI, single-process) build just returns `local` as-is. */
    double allreduce_sum(double local) const;
};

#endif

