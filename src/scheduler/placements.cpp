#include "compute_fabric/scheduler/placements.h"

namespace cf {

const char* scheduler_policy_name(SchedulerPolicy p) {
  switch (p) {
    case SchedulerPolicy::Baseline: return "baseline";
    case SchedulerPolicy::CostAware: return "cost_aware";
    default: return "unknown";
  }
}

bool parse_scheduler_policy(const std::string& s, SchedulerPolicy& out) {
  if (s == "baseline") { out = SchedulerPolicy::Baseline; return true; }
  if (s == "cost_aware") { out = SchedulerPolicy::CostAware; return true; }
  return false;
}

}  // namespace cf