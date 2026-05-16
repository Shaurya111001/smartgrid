#ifndef GRID_HPP
#define GRID_HPP

#include <vector>
#include "district.hpp"

class Grid {

private:
    std::vector<District*> districts;

public:
    Grid();

    void add_district(District* district);
    std::vector<District*>& get_districts();

    void run_simulation(int steps);
};

#endif