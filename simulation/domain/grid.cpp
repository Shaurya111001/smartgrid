#include "grid.hpp"
#include <iostream>

Grid::Grid() {}

void Grid::add_district(District* district) {
    districts.push_back(district);
}


std::vector<District*>& Grid::get_districts() {
    return districts;
}

void Grid::run_simulation(int steps) {

    for (int step = 0; step < steps; step++) {

        std::cout << "\nStep " << step + 1 << std::endl;

        for (auto district : districts) {
            district->simulate_step(step);
        }

    }
}