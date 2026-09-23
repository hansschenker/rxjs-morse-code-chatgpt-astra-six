import { map } from 'rxjs';
import type { Observable, OperatorFunction, SchedulerLike } from 'rxjs';
import type { Signal } from './encoder.js';
import { decodeMorse } from './morse-decoder.js';
import type { KeyAction, MorseEvent } from './morse-decoder.js';

/** Only the signal level crosses this boundary; no letters or durations do. */
export function signalToKeyAction(signal: Signal): KeyAction {
  return signal === 'on' ? 'press' : 'release';
}

/** Connect any timed on/off source to the same decoder used by the hand key. */
export function decodeSignals(
  unitMs: number, scheduler: SchedulerLike,
): OperatorFunction<Signal, MorseEvent> {
  function receive(source: Observable<Signal>): Observable<MorseEvent> {
    return source.pipe(map(signalToKeyAction), decodeMorse(unitMs, scheduler));
  }
  return receive;
}
