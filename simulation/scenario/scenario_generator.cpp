#include "scenario_generator.hpp"

#include "../domain/producer.hpp"
#include "../domain/consumer.hpp"
#include "../domain/accumulator.hpp"

Grid ScenarioGenerator::generate_sparse_grid(int district_count) {

    Grid grid;

    int node_id = 1;

    for (int i = 0; i < district_count; i++) {

        District* d = new District(i + 1);

        for (int p = 0; p < 3; p++)
            d->add_node(new Producer(node_id++, i + 1, 100));

        for (int c = 0; c < 5; c++)
            d->add_node(new Consumer(node_id++, i + 1, 60));

        for (int a = 0; a < 2; a++)
            d->add_node(new Accumulator(node_id++, i + 1, 500, 200));

        grid.add_district(d);
    }

    return grid;
}

Grid ScenarioGenerator::generate_dense_grid(int district_count) {

    Grid grid;

    int node_id = 1;

    for (int i = 0; i < district_count; i++) {

        District* d = new District(i + 1);

        for (int p = 0; p < 300; p++)
            d->add_node(new Producer(node_id++, i + 1, 100));

        for (int c = 0; c < 600; c++)
            d->add_node(new Consumer(node_id++, i + 1, 60));

        for (int a = 0; a < 100; a++)
            d->add_node(new Accumulator(node_id++, i + 1, 500, 200));

        grid.add_district(d);
    }

    return grid;
}
