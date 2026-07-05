// The avoid_print rule is disabled because this is a CLI tool that
// intentionally reports coverage results to stdout.
// ignore_for_file: avoid_print
import 'dart:io';

/// Reads an LCOV file and exits 1 if total coverage is below the threshold.
///
/// Usage: dart tool/check_coverage.dart coverage/lcov.info 95
void main(List<String> args) {
  if (args.length != 2) {
    print('Usage: dart tool/check_coverage.dart <lcov-file> <threshold>');
    exit(1);
  }

  final file = File(args[0]);
  if (!file.existsSync()) {
    print('LCOV file not found: ${args[0]}');
    exit(1);
  }

  final threshold = int.parse(args[1]);
  final lines = file.readAsLinesSync();

  var totalFound = 0;
  var totalHit = 0;
  String? currentFile;
  final fileStats = <String, (int found, int hit)>{};

  for (final line in lines) {
    if (line.startsWith('SF:')) {
      currentFile = line.substring(3);
    } else if (line.startsWith('LF:')) {
      final found = int.parse(line.substring(3));
      totalFound += found;
      if (currentFile != null) {
        final prev = fileStats[currentFile] ?? (0, 0);
        fileStats[currentFile] = (prev.$1 + found, prev.$2);
      }
    } else if (line.startsWith('LH:')) {
      final hit = int.parse(line.substring(3));
      totalHit += hit;
      if (currentFile != null) {
        final prev = fileStats[currentFile] ?? (0, 0);
        fileStats[currentFile] = (prev.$1, prev.$2 + hit);
      }
    }
  }

  if (totalFound == 0) {
    print('No coverage data found.');
    exit(1);
  }

  final percentage = (totalHit * 100) ~/ totalFound;
  print('Coverage: $totalHit/$totalFound ($percentage%)');

  if (percentage < threshold) {
    print('\nFAILED: Coverage $percentage% is below threshold $threshold%\n');
    print('Per-file breakdown:');
    print('${'File'.padRight(60)} ${'Lines'.padRight(10)} Coverage');
    print('-' * 80);
    for (final entry in fileStats.entries) {
      final found = entry.value.$1;
      final hit = entry.value.$2;
      final pct = found > 0 ? (hit * 100) ~/ found : 100;
      final marker = pct < threshold ? ' <---' : '';
      final lineCount = '$hit/$found';
      print(
        '${entry.key.padRight(60)} '
        '${lineCount.padRight(10)} '
        '$pct%$marker',
      );
    }
    exit(1);
  }

  print('PASSED: Coverage $percentage% >= $threshold% threshold');
}
