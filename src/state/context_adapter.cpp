#include "compute_fabric/state/context_adapter.h"

namespace cf {

ContextStateProvider* ContextAdapterManager::get() const { return provider_; }

void ContextAdapterManager::set(ContextStateProvider* provider) {
  provider_ = provider;
}

}  // namespace cf