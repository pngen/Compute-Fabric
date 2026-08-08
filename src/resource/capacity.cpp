#include "compute_fabric/resource/capacity.h"

namespace cf {

const char* node_health_name(NodeHealth h) {
  switch (h) {
    case NodeHealth::Online: return "online";
    case NodeHealth::Suspect: return "suspect";
    case NodeHealth::Draining: return "draining";
    case NodeHealth::Offline: return "offline";
    default: return "unknown";
  }
}

bool parse_node_health(const std::string& s, NodeHealth& out) {
  for (int i = 0; i <= static_cast<int>(NodeHealth::Offline); ++i) {
    if (node_health_name(static_cast<NodeHealth>(i)) == s) {
      out = static_cast<NodeHealth>(i);
      return true;
    }
  }
  return false;
}

}  // namespace cf