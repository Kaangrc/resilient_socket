import 'package:resilient_socket/src/subscription/subscription_spec.dart';

final class _RegisteredSpec {
  _RegisteredSpec(this.spec, this.order);
  final SubscriptionSpec spec;
  final int order;
}

/// Internal tracking registry for active WebSocket subscriptions.
class SubscriptionRegistry {
  final Map<String, _RegisteredSpec> _specs = {};
  int _insertionCounter = 0;

  /// Registers a new subscription [spec].
  ///
  /// Throws a [StateError] if a subscription with the same [SubscriptionSpec.id]
  /// is already registered.
  void register(SubscriptionSpec spec) {
    if (_specs.containsKey(spec.id)) {
      throw StateError(
        'Subscription with id "${spec.id}" is already registered.',
      );
    }
    _specs[spec.id] = _RegisteredSpec(spec, _insertionCounter++);
  }

  /// Removes the subscription with [id] if it exists. Silent no-op if unknown.
  void unregister(String id) {
    _specs.remove(id);
  }

  /// Returns whether a subscription with [id] is currently registered.
  bool contains(String id) => _specs.containsKey(id);

  /// Returns the registered [SubscriptionSpec] for [id], or `null` if not registered.
  SubscriptionSpec? get(String id) => _specs[id]?.spec;

  /// Returns the list of active subscriptions sorted stably by ascending priority,
  /// then by ascending insertion order.
  List<SubscriptionSpec> get active {
    final entries = _specs.values.toList()
      ..sort((a, b) {
        final priorityDiff = a.spec.priority.compareTo(b.spec.priority);
        if (priorityDiff != 0) {
          return priorityDiff;
        }
        return a.order.compareTo(b.order);
      });
    return entries.map((e) => e.spec).toList();
  }

  /// Clears all registered subscriptions and resets insertion tracking.
  void clear() {
    _specs.clear();
    _insertionCounter = 0;
  }
}
