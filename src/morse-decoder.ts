import {
  concat, concatMap, defer, distinctUntilChanged, filter, map, merge, of,
  pairwise, scan, startWith, switchMap, timer, timestamp,
} from 'rxjs';
import type { Observable, OperatorFunction, SchedulerLike, Timestamp } from 'rxjs';
import { morseAlphabet } from './alphabet.js';

import type { Mark } from './alphabet.js';
export type { Mark } from './alphabet.js';
export type KeyAction = 'press' | 'release' | 'cancel' | 'reset';

export type SymbolResult =
  | Readonly<{ ok: true; code: string; letter: string }>
  | Readonly<{ ok: false; code: string }>;

/** Pure lookup. The original table's empty translation is reported as unknown. */
export function decodeSymbol(code: string): SymbolResult {
  const letter = morseAlphabet[code];
  return typeof letter !== 'string' || letter === ''
    ? { ok: false, code }
    : { ok: true, code, letter };
}

export type MorseEvent =
  | Readonly<{ kind: 'press'; at: number }>
  | Readonly<{ kind: 'mark'; at: number; mark: Mark; durationMs: number }>
  | Readonly<{ kind: 'letter'; at: number; code: string; letter: string }>
  | Readonly<{ kind: 'word-gap'; at: number }>
  | Readonly<{ kind: 'invalid-code'; at: number; code: string }>
  | Readonly<{
      kind: 'input-error'; at: number; code: string;
      reason: 'non-positive-pulse' | 'unfinished-press';
    }>
  | Readonly<{ kind: 'cancel' | 'reset'; at: number }>;

export type MorseTiming = Readonly<{
  dotMaxMs: number;
  letterSilenceMs: number;
  wordSilenceMs: number;
}>;

export type MachineInput =
  | Readonly<{ kind: KeyAction | 'end'; at: number }>
  | Readonly<{ kind: 'silence'; at: number; releasedAt: number }>;

export type DecoderMemory = Readonly<{
  pressedAt: number | null;
  releasedAt: number | null;
  code: string;
  wordPending: boolean;
  events: readonly MorseEvent[];
}>;

export const initialDecoderMemory: DecoderMemory = Object.freeze({
  pressedAt: null, releasedAt: null, code: '', wordPending: false, events: [],
});

/** The threshold is inclusive: at dotMaxMs the pulse is still a dot. */
export function classifyMark(durationMs: number, dotMaxMs: number): Mark {
  return durationMs <= dotMaxMs ? '.' : '-';
}

function finishLetter(state: DecoderMemory, at: number): DecoderMemory {
  if (state.code === '') return state;
  const decoded = decodeSymbol(state.code);
  const event: MorseEvent = decoded.ok
    ? { kind: 'letter', at, code: decoded.code, letter: decoded.letter }
    : { kind: 'invalid-code', at, code: decoded.code };
  return { ...state, code: '', wordPending: true, events: [...state.events, event] };
}

function finishWord(state: DecoderMemory, at: number): DecoderMemory {
  return state.wordPending
    ? { ...state, wordPending: false, events: [...state.events, { kind: 'word-gap', at }] }
    : state;
}

/**
 * Both timer wake-ups and a new press use the SAME elapsed-silence rules.
 * This also resolves a press arriving at exactly a boundary, regardless of
 * whether the source event or the timer is delivered first in that frame.
 */
function settleSilence(
  timing: MorseTiming, state: DecoderMemory, at: number,
): DecoderMemory {
  if (state.pressedAt !== null || state.releasedAt === null) return state;
  const elapsed = at - state.releasedAt;
  const letterSettled = elapsed >= timing.letterSilenceMs
    ? finishLetter(state, at)
    : state;
  return elapsed >= timing.wordSilenceMs
    ? finishWord(letterSettled, at)
    : letterSettled;
}

/** Pure transition function: previous memory + one input -> new memory/events. */
export function reduceMorse(
  timing: MorseTiming, previous: DecoderMemory, input: MachineInput,
): DecoderMemory {
  const state: DecoderMemory = { ...previous, events: [] };

  switch (input.kind) {
    case 'press': {
      if (state.pressedAt !== null) return state;
      const ready = settleSilence(timing, state, input.at);
      return {
        ...ready, pressedAt: input.at,
        events: [...ready.events, { kind: 'press', at: input.at }],
      };
    }
    case 'release': {
      if (state.pressedAt === null) return state;
      const durationMs = input.at - state.pressedAt;
      const released = { ...state, pressedAt: null, releasedAt: input.at };
      if (durationMs <= 0) {
        return {
          ...released, code: '',
          events: [{ kind: 'input-error', at: input.at, code: state.code, reason: 'non-positive-pulse' }],
        };
      }
      const mark = classifyMark(durationMs, timing.dotMaxMs);
      return {
        ...released, code: state.code + mark,
        events: [{ kind: 'mark', at: input.at, mark, durationMs }],
      };
    }
    case 'silence':
      return input.releasedAt === state.releasedAt
        ? settleSilence(timing, state, input.at)
        : state;
    case 'cancel':
    case 'reset':
      return {
        ...initialDecoderMemory,
        events: [{ kind: input.kind, at: input.at }],
      };
    case 'end':
      return state.pressedAt === null ? state : {
        ...initialDecoderMemory,
        events: [{ kind: 'input-error', at: input.at, code: state.code, reason: 'unfinished-press' }],
      };
  }
}

function inputFromTimestamp(event: Timestamp<KeyAction>): MachineInput {
  return { kind: event.value, at: event.timestamp };
}

function isPairedAction([previous, current]: [KeyAction, KeyAction]): boolean {
  return current !== 'release' || previous === 'press';
}

function currentAction([, current]: [KeyAction, KeyAction]): KeyAction {
  return current;
}

function emittedEvents(state: DecoderMemory): readonly MorseEvent[] {
  return state.events;
}

function checkedTiming(
  dotMaxMs: number, letterSilenceMs: number, wordSilenceMs: number,
): MorseTiming {
  const values = [dotMaxMs, letterSilenceMs, wordSilenceMs];
  if (!values.every(Number.isFinite)
    || dotMaxMs <= 0 || letterSilenceMs <= 0
    || wordSilenceMs <= letterSilenceMs || wordSilenceMs > 2_147_483_647) {
    throw new RangeError('Use positive finite thresholds; word silence must exceed letter silence and be <= 2147483647 ms.');
  }
  return Object.freeze({ dotMaxMs, letterSilenceMs, wordSilenceMs });
}

/**
 * Explicit recognition thresholds, in milliseconds. No subscriptions are made
 * until the returned dataflow is subscribed. Every subscription has fresh state.
 * Input arrival times and silence timers use this same scheduler.
 */
export function decodeMorseWith(
  dotMaxMs: number,
  letterSilenceMs: number,
  wordSilenceMs: number,
  scheduler: SchedulerLike,
): OperatorFunction<KeyAction, MorseEvent> {
  function decodeSource(source: Observable<KeyAction>): Observable<MorseEvent> {
    function createSession(): Observable<MorseEvent> {
      const timing = checkedTiming(dotMaxMs, letterSilenceMs, wordSilenceMs);

      function withSilenceChecks(input: MachineInput): Observable<MachineInput> {
        if (input.kind !== 'release') return of(input);

        function silenceNow(): MachineInput {
          return { kind: 'silence', at: scheduler.now(), releasedAt: input.at };
        }

        // The two deadlines both start at release, rather than after each other.
        const letterDeadline$ = timer(timing.letterSilenceMs, scheduler).pipe(map(silenceNow));
        const wordDeadline$ = timer(timing.wordSilenceMs, scheduler).pipe(map(silenceNow));
        return concat(of(input), merge(letterDeadline$, wordDeadline$));
      }

      function advance(state: DecoderMemory, input: MachineInput): DecoderMemory {
        return reduceMorse(timing, state, input);
      }

      function sourceEnded(): Observable<MachineInput> {
        return of({ kind: 'end', at: scheduler.now() });
      }

      const inputs$ = source.pipe(
        startWith<KeyAction>('cancel'), // Initial physical state: released.
        distinctUntilChanged(), // Ignore auto-repeat and duplicate releases.
        pairwise(),
        filter(isPairedAction), // An orphan release must not start idle timers.
        map(currentAction),
        timestamp(scheduler),
        map(inputFromTimestamp),
        switchMap(withSilenceChecks), // A new action cancels obsolete deadlines.
      );

      // Normal completion waits for the current silence checks. Unsubscription
      // cancels them immediately and emits no fabricated final boundary.
      return concat(inputs$, defer(sourceEnded)).pipe(
        scan(advance, initialDecoderMemory),
        concatMap(emittedEvents),
      );
    }
    return defer(createSession);
  }
  return decodeSource;
}

/**
 * Unit-based policy: dot <= 2 units, dash > 2; letters after 3 silent units,
 * words after 7. The 2-unit threshold separates ideal 1- and 3-unit presses.
 */
export function decodeMorse(
  unitMs: number, scheduler: SchedulerLike,
): OperatorFunction<KeyAction, MorseEvent> {
  return decodeMorseWith(2 * unitMs, 3 * unitMs, 7 * unitMs, scheduler);
}

export type Transcript = Readonly<{
  text: string;
  currentCode: string;
  pressed: boolean;
  pendingSpace: boolean;
  lastMark: Mark | null;
  lastDurationMs: number | null;
  issue: string | null;
}>;

export const initialTranscript: Transcript = Object.freeze({
  text: '', currentCode: '', pressed: false, pendingSpace: false,
  lastMark: null, lastDurationMs: null, issue: null,
});

function appendLetter(state: Transcript, letter: string): string {
  const separator = state.pendingSpace && state.text !== '' ? ' ' : '';
  return state.text + separator + letter;
}

/** Pure UI/text projection. Spaces appear only when the next letter arrives. */
export function reduceTranscript(state: Transcript, event: MorseEvent): Transcript {
  switch (event.kind) {
    case 'press':
      return { ...state, pressed: true };
    case 'mark':
      return {
        ...state, pressed: false, currentCode: state.currentCode + event.mark,
        lastMark: event.mark, lastDurationMs: event.durationMs,
      };
    case 'letter':
      return {
        ...state, text: appendLetter(state, event.letter), currentCode: '',
        pendingSpace: false, issue: null,
      };
    case 'word-gap':
      return { ...state, pendingSpace: true };
    case 'invalid-code':
      return {
        ...state, text: appendLetter(state, '\uFFFD'), currentCode: '',
        pendingSpace: false, issue: `Unknown Morse sequence: ${event.code}`,
      };
    case 'input-error':
      return {
        ...state, pressed: false, currentCode: '',
        issue: event.reason === 'unfinished-press'
          ? 'Input ended while the key was still held; the unfinished letter was discarded.'
          : 'A press must have a positive duration; the unfinished letter was discarded.',
      };
    case 'cancel':
      return {
        ...state, pressed: false, currentCode: '', pendingSpace: state.text !== '',
        issue: null,
      };
    case 'reset':
      return initialTranscript;
  }
}
