import {
  concat,
  concatMap,
  defer,
  from,
  ignoreElements,
  map,
  of,
  timer,
} from 'rxjs';
import type { Observable, OperatorFunction, SchedulerLike } from 'rxjs';

import { encodingTable } from './alphabet.js';
import type { Mark, MorseCode } from './alphabet.js';

export type { Mark, MorseCode, MorseTree } from './alphabet.js';
export { foldTree, morseTree } from './alphabet.js';
export type MorseWord = readonly MorseCode[];
export type MorseMessage = readonly MorseWord[];

export type UnsupportedCharacter = Readonly<{
  character: string;
  /** Zero-based Unicode code-point position in the original input. */
  position: number;
}>;

export type EncodeResult =
  | Readonly<{ ok: true; value: MorseMessage }>
  | Readonly<{ ok: false; errors: readonly UnsupportedCharacter[] }>;

type Token =
  | Readonly<{ kind: 'letter'; code: MorseCode }>
  | Readonly<{ kind: 'space' }>
  | Readonly<{ kind: 'invalid'; problem: UnsupportedCharacter }>;

function readCharacter(character: string, position: number): Token {
  if (/\s/u.test(character)) return { kind: 'space' };

  // ASCII case folding avoids silently turning, for example, ß into SS.
  const normalized = /^[a-z]$/u.test(character)
    ? character.toUpperCase()
    : character;
  const code = encodingTable[normalized];

  return code === undefined
    ? { kind: 'invalid', problem: { character, position } }
    : { kind: 'letter', code };
}

function problemsFromToken(token: Token): readonly UnsupportedCharacter[] {
  return token.kind === 'invalid' ? [token.problem] : [];
}

function appendToken(words: MorseMessage, token: Token): MorseMessage {
  const currentWord = words.at(-1) ?? [];

  if (token.kind === 'letter') {
    return [...words.slice(0, -1), [...currentWord, token.code]];
  }

  return token.kind === 'space' && currentWord.length > 0
    ? [...words, []]
    : words;
}

function hasLetters(word: MorseWord): boolean {
  return word.length > 0;
}

/** Pure, total text encoding: unsupported input is returned as data. */
export function encodeText(text: string): EncodeResult {
  const tokens = Array.from(text).map(readCharacter);
  const errors = tokens.flatMap(problemsFromToken);

  if (errors.length > 0) return { ok: false, errors };

  const value = tokens.reduce<MorseMessage>(appendToken, [[]]).filter(hasLetters);
  return { ok: true, value };
}

function formatCode(code: MorseCode): string {
  return code.join('');
}

function formatWord(word: MorseWord): string {
  return word.map(formatCode).join(' ');
}

/** Spaces separate letters; a slash separates words in the written display. */
export function formatMorse(message: MorseMessage): string {
  return message.map(formatWord).join(' / ');
}

/** Every source text becomes one result. Source timing and sharing are retained. */
export function encodeMorse(): OperatorFunction<string, EncodeResult> {
  return map(encodeText);
}

export type Signal = 'on' | 'off';
export type Segment =
  | Readonly<{ signal: 'on'; units: 1 | 3 }>
  | Readonly<{ signal: 'off'; units: 1 | 3 | 7 }>;

const elementGap: Segment = Object.freeze({ signal: 'off', units: 1 });
const letterGap: Segment = Object.freeze({ signal: 'off', units: 3 });
const wordGap: Segment = Object.freeze({ signal: 'off', units: 7 });

function intersperse<A>(separator: A, values: readonly A[]): readonly A[] {
  function insertSeparator(value: A, index: number): readonly A[] {
    return index === 0 ? [value] : [separator, value];
  }
  return values.flatMap(insertSeparator);
}

function markToSegment(mark: Mark): Segment {
  return { signal: 'on', units: mark === '.' ? 1 : 3 };
}

function codeToSegments(code: MorseCode): readonly Segment[] {
  return intersperse(elementGap, code.map(markToSegment));
}

function wordToSegments(word: MorseWord): readonly Segment[] {
  return intersperse<readonly Segment[]>(
    [letterGap], word.map(codeToSegments),
  ).flat();
}

/** Pure timing description. Gaps are inserted only BETWEEN elements. */
export function toSegments(message: MorseMessage): readonly Segment[] {
  return intersperse<readonly Segment[]>(
    [wordGap], message.map(wordToSegments),
  ).flat();
}

/**
 * A cold playback description. Each subscriber owns its timers.
 * Emits a signal at each segment's start, then waits for its full duration.
 * Ends with seven units of silence, so queued messages remain separated.
 * Empty messages emit off and complete immediately.
 * Unsubscribing cancels the active timer and discards queued segments.
 * A hardware/UI sink must also reset itself on teardown; a stopped observer
 * cannot receive a final off notification.
 */
export function playMorse(
  message: MorseMessage,
  unitMs: number,
  scheduler: SchedulerLike,
): Observable<Signal> {
  function holdSegment(segment: Segment): Observable<Signal> {
    return concat(
      of(segment.signal),
      timer(segment.units * unitMs, scheduler).pipe(ignoreElements()),
    );
  }

  function startPlayback(): Observable<Signal> {
    // Keep every timer delay inside the usual JavaScript timer range.
    if (!Number.isFinite(unitMs) || unitMs <= 0 || unitMs > 2_147_483_647 / 7) {
      throw new RangeError('unitMs must be positive, finite, and at most 2147483647 / 7.');
    }

    const segments = toSegments(message);
    return segments.length === 0
      ? of<Signal>('off')
      : from([...segments, wordGap]).pipe(concatMap(holdSegment));
  }

  return defer(startPlayback);
}
