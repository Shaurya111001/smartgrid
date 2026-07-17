#ifndef SCENARIO_GENERATOR_HPP
#define SCENARIO_GENERATOR_HPP

#include "../domain/grid.hpp"

class ScenarioGenerator {

public:

    static Grid generate_sparse_grid(int districts);

    static Grid generate_dense_grid(int districts);

};

#endif
