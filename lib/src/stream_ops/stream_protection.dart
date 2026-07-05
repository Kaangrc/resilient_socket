import 'dart:async';

class _ProtectionController<T> {
  _ProtectionController(
    void Function(_ProtectionController<T> wrapper) onListen,
  ) {
    controller = StreamController<T>(
      sync: true,
      onListen: () => onListen(this),
      onCancel: () {
        timer?.cancel();
        timer = null;
        final sub = subscription;
        subscription = null;
        return sub?.cancel();
      },
    );
  }

  late final StreamController<T> controller;
  StreamSubscription<T>? subscription;
  Timer? timer;

  void add(T event) {
    if (!controller.isClosed) controller.add(event);
  }

  void addError(Object error, [StackTrace? stackTrace]) {
    if (!controller.isClosed) controller.addError(error, stackTrace);
  }

  void dispose() {
    timer?.cancel();
    timer = null;
    unawaited(subscription?.cancel());
    subscription = null;
    unawaited(controller.close());
  }
}

/// Extension providing single-subscription stream protection and throttling
/// operators for resilient WebSocket messaging.
extension StreamProtection<T> on Stream<T> {
  /// Emits the first event immediately and opens a window of [window] duration.
  /// Subsequent events within the window overwrite a pending slot. At the end
  /// of the window, if a pending event exists, it is emitted and a new window
  /// is opened; otherwise, the operator transitions to idle.
  Stream<T> throttleLatest(Duration window) {
    var hasPending = false;
    T? pendingValue;
    var isWindowOpen = false;

    late _ProtectionController<T> wrapper;
    void onWindowEnd() {
      wrapper.timer = null;
      if (hasPending && !wrapper.controller.isClosed) {
        final val = pendingValue as T;
        hasPending = false;
        pendingValue = null;
        wrapper
          ..add(val)
          ..timer = Timer(window, onWindowEnd);
      } else {
        isWindowOpen = false;
      }
    }

    wrapper = _ProtectionController<T>((w) {
      w.subscription = listen(
        (event) {
          if (!isWindowOpen) {
            w
              ..add(event)
              ..timer = Timer(window, onWindowEnd);
            isWindowOpen = true;
          } else {
            hasPending = true;
            pendingValue = event;
          }
        },
        onError: w.addError,
        onDone: () {
          if (hasPending && !w.controller.isClosed) {
            w.add(pendingValue as T);
            hasPending = false;
            pendingValue = null;
          }
          w.dispose();
        },
      );
    });

    return wrapper.controller.stream;
  }

  /// (Re)arms a timer of [quiet] duration on every event. Emits the most
  /// recent event only after [quiet] duration has elapsed with no new events.
  Stream<T> debounceQuiet(Duration quiet) {
    var hasValue = false;
    T? lastValue;

    late _ProtectionController<T> wrapper;
    wrapper = _ProtectionController<T>((w) {
      w.subscription = listen(
        (event) {
          hasValue = true;
          lastValue = event;
          w.timer?.cancel();
          w.timer = Timer(quiet, () {
            w.timer = null;
            if (hasValue && !w.controller.isClosed) {
              final val = lastValue as T;
              hasValue = false;
              lastValue = null;
              w.add(val);
            }
          });
        },
        onError: w.addError,
        onDone: () {
          if (hasValue && !w.controller.isClosed) {
            w.add(lastValue as T);
            hasValue = false;
            lastValue = null;
          }
          w.dispose();
        },
      );
    });

    return wrapper.controller.stream;
  }

  /// Conflates incoming events within rolling [window] intervals. The first event
  /// arms a window timer and seeds the accumulator. Subsequent events are combined
  /// using [merge] (or overwrite if [merge] is null). When the timer fires, the
  /// accumulated value is emitted and the timer disarms.
  Stream<T> conflate(
    Duration window, {
    T Function(T previous, T next)? merge,
  }) {
    var hasValue = false;
    T? acc;

    late _ProtectionController<T> wrapper;
    wrapper = _ProtectionController<T>((w) {
      w.subscription = listen(
        (event) {
          if (!hasValue) {
            hasValue = true;
            acc = event;
          } else {
            acc = merge != null ? merge(acc as T, event) : event;
          }
          w.timer ??= Timer(window, () {
            w.timer = null;
            if (hasValue && !w.controller.isClosed) {
              final val = acc as T;
              hasValue = false;
              acc = null;
              w.add(val);
            }
          });
        },
        onError: w.addError,
        onDone: () {
          if (hasValue && !w.controller.isClosed) {
            w.add(acc as T);
            hasValue = false;
            acc = null;
          }
          w.dispose();
        },
      );
    });

    return wrapper.controller.stream;
  }

  /// Samples the latest unseen value every [period]. Starts a periodic timer
  /// upon listening; each tick emits the latest unseen value if one arrived
  /// during the period.
  Stream<T> sampleEvery(Duration period) {
    var hasUnseen = false;
    T? latestValue;

    late _ProtectionController<T> wrapper;
    wrapper = _ProtectionController<T>((w) {
      w
        ..timer = Timer.periodic(period, (_) {
          if (hasUnseen && !w.controller.isClosed) {
            final val = latestValue as T;
            hasUnseen = false;
            latestValue = null;
            w.add(val);
          }
        })
        ..subscription = listen(
          (event) {
            hasUnseen = true;
            latestValue = event;
          },
          onError: w.addError,
          onDone: () {
            if (hasUnseen && !w.controller.isClosed) {
              w.add(latestValue as T);
              hasUnseen = false;
              latestValue = null;
            }
            w.dispose();
          },
        );
    });

    return wrapper.controller.stream;
  }
}
