import 'package:meta/meta.dart';

/// Immutable specification for a persistent WebSocket subscription.
@immutable
final class SubscriptionSpec {
  /// Creates a subscription specification.
  const SubscriptionSpec({
    required this.id,
    required this.subscribeMessage,
    this.unsubscribeMessage,
    this.priority = 0,
  });

  /// Unique identifier for this subscription.
  final String id;

  /// Function generating the outbound subscribe message payload.
  final Object Function() subscribeMessage;

  /// Optional function generating the outbound unsubscribe message payload.
  final Object Function()? unsubscribeMessage;

  /// Priority of the subscription (lower values execute earlier during replay).
  final int priority;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SubscriptionSpec &&
          runtimeType == other.runtimeType &&
          id == other.id &&
          subscribeMessage == other.subscribeMessage &&
          unsubscribeMessage == other.unsubscribeMessage &&
          priority == other.priority;

  @override
  int get hashCode =>
      id.hashCode ^
      subscribeMessage.hashCode ^
      unsubscribeMessage.hashCode ^
      priority.hashCode;

  @override
  String toString() => 'SubscriptionSpec(id: $id, priority: $priority)';
}
