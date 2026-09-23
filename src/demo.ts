import { asyncScheduler, distinctUntilChanged, filter, map, scan } from 'rxjs';
import {
  decodeSignals, encodeText, initialTranscript, playMorse, reduceTranscript,
} from './index.js';
import type { Transcript } from './index.js';

function decodedText(state: Transcript): string {
  return state.text;
}

function hasText(text: string): boolean {
  return text.length > 0;
}

function printText(text: string): void {
  console.log(text);
}

function finished(): void {
  console.log('Round trip complete.');
}

const message = encodeText('SOS HELP');
if (!message.ok) throw new Error('The demonstration message must be valid.');

// The decoder sees only press/release arrivals. It does not receive the text,
// the transmitter's Morse symbols, or its planned segment durations.
const decoded$ = playMorse(message.value, 20, asyncScheduler).pipe(
  decodeSignals(20, asyncScheduler),
  scan(reduceTranscript, initialTranscript),
  map(decodedText),
  distinctUntilChanged(),
  filter(hasText),
);

decoded$.subscribe({ next: printText, complete: finished });
