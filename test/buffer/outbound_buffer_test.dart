import 'package:fake_async/fake_async.dart';
import 'package:resilient_socket/resilient_socket.dart';
import 'package:resilient_socket/src/buffer/outbound_buffer.dart';
import 'package:test/test.dart';

void main() {
  group('OutboundBuffer', () {
    test('1. FIFO drain order preserved', () {
      fakeAsync((async) {
        final drops = <Object>[];
        final buffer =
            OutboundBuffer(
                const OutboundBufferOptions(),
                onDrop: (reason, count) => drops.add((reason, count)),
              )
              ..enqueue('a')
              ..enqueue('b')
              ..enqueue('c');

        expect(buffer.length, equals(3));

        final drained = buffer.drain();
        expect(drained.length, equals(3));
        expect(drained[0].payload, equals('a'));
        expect(drained[1].payload, equals('b'));
        expect(drained[2].payload, equals('c'));
        expect(buffer.length, equals(0));
        expect(drops, isEmpty);
      });
    });

    test('2. TTL expiration and strict boundary (> ttl is expired)', () {
      fakeAsync((async) {
        final drops = <(BufferDropReason, int)>[];
        final buffer = OutboundBuffer(
          const OutboundBufferOptions(
            defaultTtl: Duration(seconds: 10),
          ),
          onDrop: (reason, count) => drops.add((reason, count)),
        )..enqueue('msg1');

        // Boundary: exactly 10s is NOT expired (strict >)
        async.elapse(const Duration(seconds: 10));
        var drained = buffer.drain();
        expect(drained.length, equals(1));
        expect(drained[0].payload, equals('msg1'));
        expect(drops, isEmpty);

        // Re-enqueue and test expiration at 10.001s (10s + 1ms)
        buffer.enqueue('msg2');
        async.elapse(const Duration(milliseconds: 10001));

        drained = buffer.drain();
        expect(drained, isEmpty);
        expect(drops.length, equals(1));
        expect(drops[0], equals((BufferDropReason.ttlExpired, 1)));
      });
    });

    test('3. Per-message ttl overrides defaultTtl', () {
      fakeAsync((async) {
        final drops = <(BufferDropReason, int)>[];
        final buffer =
            OutboundBuffer(
                const OutboundBufferOptions(),
                onDrop: (reason, count) => drops.add((reason, count)),
              )
              ..enqueue('default_ttl_msg')
              ..enqueue('short_ttl_msg', ttl: const Duration(seconds: 5));

        async.elapse(const Duration(seconds: 6));

        final drained = buffer.drain();
        expect(drained.length, equals(1));
        expect(drained[0].payload, equals('default_ttl_msg'));
        expect(drops.length, equals(1));
        expect(drops[0], equals((BufferDropReason.ttlExpired, 1)));
      });
    });

    test('4. dropOldest at cap 3: evicts index 0 on 4th enqueue', () {
      fakeAsync((async) {
        final drops = <(BufferDropReason, int)>[];
        final buffer =
            OutboundBuffer(
                const OutboundBufferOptions(
                  maxMessages: 3,
                ),
                onDrop: (reason, count) => drops.add((reason, count)),
              )
              ..enqueue(1)
              ..enqueue(2)
              ..enqueue(3);
        expect(drops, isEmpty);

        buffer.enqueue(4);
        expect(drops.length, equals(1));
        expect(drops[0], equals((BufferDropReason.overflow, 1)));

        final drained = buffer.drain();
        expect(drained.map((m) => m.payload).toList(), equals([2, 3, 4]));
      });
    });

    test('5. dropNewest at cap 3: discards incoming on 4th enqueue', () {
      fakeAsync((async) {
        final drops = <(BufferDropReason, int)>[];
        final buffer =
            OutboundBuffer(
                const OutboundBufferOptions(
                  maxMessages: 3,
                  overflow: OverflowStrategy.dropNewest,
                ),
                onDrop: (reason, count) => drops.add((reason, count)),
              )
              ..enqueue(1)
              ..enqueue(2)
              ..enqueue(3);
        expect(drops, isEmpty);

        buffer.enqueue(4);
        expect(drops.length, equals(1));
        expect(drops[0], equals((BufferDropReason.overflow, 1)));

        final drained = buffer.drain();
        expect(drained.map((m) => m.payload).toList(), equals([1, 2, 3]));
      });
    });

    test(
      '6. dropByPriority at cap 3: evicts lowest priority and respects FIFO fairness ties',
      () {
        fakeAsync((async) {
          final drops = <(BufferDropReason, int)>[];
          final buffer =
              OutboundBuffer(
                  const OutboundBufferOptions(
                    maxMessages: 3,
                    overflow: OverflowStrategy.dropByPriority,
                  ),
                  onDrop: (reason, count) => drops.add((reason, count)),
                )
                ..enqueue('m0')
                ..enqueue('m5_a', priority: 5)
                ..enqueue('m5_b', priority: 5)
                // Enqueue p=3 -> evicts lowest (p=0)
                ..enqueue('m3', priority: 3);
          expect(drops.length, equals(1));
          expect(drops[0], equals((BufferDropReason.overflow, 1)));
          expect(
            buffer.drain().map((m) => m.payload).toList(),
            equals(['m5_a', 'm5_b', 'm3']),
          );

          // Repopulate buffer with priorities [3, 5, 5]
          buffer
            ..enqueue('m3', priority: 3)
            ..enqueue('m5_a', priority: 5)
            ..enqueue('m5_b', priority: 5)
            // Enqueue p=1 into [3,5,5] -> incoming discarded (1 <= lowest priority 3)
            ..enqueue('m1', priority: 1);
          expect(drops.length, equals(2));
          expect(drops[1], equals((BufferDropReason.overflow, 1)));
          expect(
            buffer.drain().map((m) => m.payload).toList(),
            equals(['m3', 'm5_a', 'm5_b']),
          );

          // Test equal priority tie displacement: repopulate with [5, 5, 5]
          buffer
            ..enqueue('m5_1', priority: 5)
            ..enqueue('m5_2', priority: 5)
            ..enqueue('m5_3', priority: 5)
            // Enqueue p=5 into [5,5,5] -> incoming discarded (no tie displacement!)
            ..enqueue('m5_incoming', priority: 5);
          expect(drops.length, equals(3));
          expect(drops[2], equals((BufferDropReason.overflow, 1)));
          expect(
            buffer.drain().map((m) => m.payload).toList(),
            equals(['m5_1', 'm5_2', 'm5_3']),
          );
        });
      },
    );

    test(
      '7. throwException: 4th enqueue throws BufferOverflowException, buffer intact',
      () {
        fakeAsync((async) {
          final drops = <(BufferDropReason, int)>[];
          final buffer =
              OutboundBuffer(
                  const OutboundBufferOptions(
                    maxMessages: 3,
                    overflow: OverflowStrategy.throwException,
                  ),
                  onDrop: (reason, count) => drops.add((reason, count)),
                )
                ..enqueue(1)
                ..enqueue(2)
                ..enqueue(3);

          expect(
            () => buffer.enqueue(4),
            throwsA(
              isA<BufferOverflowException>().having(
                (e) => e.capacity,
                'capacity',
                equals(3),
              ),
            ),
          );

          expect(drops, isEmpty);
          final drained = buffer.drain();
          expect(drained.map((m) => m.payload).toList(), equals([1, 2, 3]));
        });
      },
    );

    test(
      '8. Byte cap: sizeEstimator and maxBytes enforce limits with dropOldest',
      () {
        fakeAsync((async) {
          final drops = <(BufferDropReason, int)>[];
          final buffer =
              OutboundBuffer(
                  OutboundBufferOptions(
                    maxBytes: 10,
                    sizeEstimator: (payload) => (payload as String).length,
                  ),
                  onDrop: (reason, count) => drops.add((reason, count)),
                )
                ..enqueue('aaaa') // 4 bytes
                ..enqueue('bbbb'); // 4 bytes (total 8)
          expect(drops, isEmpty);

          // Enqueue 5 bytes: 8 + 5 = 13 > 10. Evicts 'aaaa' -> remaining 4 + 5 = 9 <= 10.
          buffer.enqueue('ccccc');
          expect(drops.length, equals(1));
          expect(drops[0], equals((BufferDropReason.overflow, 1)));

          final drained = buffer.drain();
          expect(
            drained.map((m) => m.payload).toList(),
            equals(['bbbb', 'ccccc']),
          );
        });
      },
    );

    test(
      '8b. Byte cap cascading evictions: throwing away multiple small frames for a large frame',
      () {
        fakeAsync((async) {
          final drops = <(BufferDropReason, int)>[];
          final buffer =
              OutboundBuffer(
                  OutboundBufferOptions(
                    maxBytes: 10,
                    sizeEstimator: (payload) => (payload as String).length,
                  ),
                  onDrop: (reason, count) => drops.add((reason, count)),
                )
                ..enqueue('aaa') // 3 bytes
                ..enqueue('bbb') // 3 bytes (total 6)
                ..enqueue('ccc'); // 3 bytes (total 9)
          expect(drops, isEmpty);

          // Enqueue 8 bytes: 9 + 8 = 17 > 10.
          // Evict 'aaa' -> 6 + 8 = 14 > 10.
          // Evict 'bbb' -> 3 + 8 = 11 > 10.
          // Evict 'ccc' -> 0 + 8 = 8 <= 10.
          // Total evicted in a single execution: 3 messages!
          buffer.enqueue('dddddddd');
          expect(drops.length, equals(1));
          expect(drops[0], equals((BufferDropReason.overflow, 3)));

          final drained = buffer.drain();
          expect(drained.map((m) => m.payload).toList(), equals(['dddddddd']));
        });
      },
    );

    test(
      '9. clear(disposed) triggers exactly once if non-empty, never when empty',
      () {
        fakeAsync((async) {
          final drops = <(BufferDropReason, int)>[];
          final buffer = OutboundBuffer(
            const OutboundBufferOptions(),
            onDrop: (reason, count) => drops.add((reason, count)),
          )..clear(BufferDropReason.disposed);

          // Empty buffer -> onDrop NOT called
          expect(drops, isEmpty);

          // 3 messages -> onDrop called exactly once with count 3
          buffer
            ..enqueue(1)
            ..enqueue(2)
            ..enqueue(3);
          expect(buffer.length, equals(3));

          buffer.clear(BufferDropReason.disposed);
          expect(buffer.length, equals(0));
          expect(drops.length, equals(1));
          expect(drops[0], equals((BufferDropReason.disposed, 3)));

          // Clear again on empty -> onDrop NOT called
          buffer.clear(BufferDropReason.disposed);
          expect(drops.length, equals(1));
        });
      },
    );
  });
}
