#include "node.hpp"

Node::Node(uint32_t node_id, uint32_t district_id, NodeType type)
    : node_id(node_id), district_id(district_id), node_type(type) {}

uint32_t Node::get_node_id() const {
    return node_id;
}

uint32_t Node::get_district_id() const {
    return district_id;
}

NodeType Node::get_node_type() const {
    return node_type;
}
