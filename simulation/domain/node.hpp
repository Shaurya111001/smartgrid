#ifndef NODE_HPP
#define NODE_HPP

#include <cstdint>

enum class NodeType {
    PRODUCER,
    CONSUMER,
    ACCUMULATOR
};

class Node {

protected:
    uint32_t node_id;
    uint32_t district_id;
    NodeType node_type;

public:
    Node(uint32_t node_id, uint32_t district_id, NodeType type);

    virtual ~Node() = default;

    uint32_t get_node_id() const;
    uint32_t get_district_id() const;
    NodeType get_node_type() const;

    virtual double compute_energy() = 0;
};

#endif
