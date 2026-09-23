import assert from 'node:assert/strict';
import test from 'node:test';
import { concatMap, defer, finalize, of } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import {
  encodeMorse, encodeText, formatMorse, playMorse, toSegments,
} from '../dist/encoder.js';

// Independent expected codes from ITU-R M.1677-1, Annex 1 (A-Z, 0-9).
const alphabet = {
  A: '.-', B: '-...', C: '-.-.', D: '-..', E: '.', F: '..-.',
  G: '--.', H: '....', I: '..', J: '.---', K: '-.-', L: '.-..',
  M: '--', N: '-.', O: '---', P: '.--.', Q: '--.-', R: '.-.',
  S: '...', T: '-', U: '..-', V: '...-', W: '.--', X: '-..-',
  Y: '-.--', Z: '--..',
  0: '-----', 1: '.----', 2: '..---', 3: '...--', 4: '....-',
  5: '.....', 6: '-....', 7: '--...', 8: '---..', 9: '----.',
};

function encoded(text) {
  const result = encodeText(text);
  assert.equal(result.ok, true);
  return result.value;
}

function makeClock() {
  return new TestScheduler(assert.deepEqual);
}

function observe(source, clock) {
  const events = [];
  const subscription = source.subscribe({
    next(value) { events.push([clock.now(), value]); },
    error(error) { events.push([clock.now(), error]); },
    complete() { events.push([clock.now(), 'complete']); },
  });
  return { events, subscription };
}

test('all 36 characters match the independent alphabet fixture', () => {
  for (const [character, code] of Object.entries(alphabet)) {
    assert.equal(formatMorse(encoded(character)), code, character);
    assert.equal(formatMorse(encoded(character.toLowerCase())), code, character);
  }
});

test('normalizes whitespace while retaining word and letter boundaries', () => {
  assert.equal(
    formatMorse(encoded(' \t Morse\n\ncode 123\r\n')),
    '-- --- .-. ... . / -.-. --- -.. . / .---- ..--- ...--',
  );
});

test('empty and whitespace-only text encode as empty messages', () => {
  assert.deepEqual(encoded(''), []);
  assert.deepEqual(encoded(' \t\n'), []);
});

test('reports every unsupported character using original code-point positions', () => {
  assert.deepEqual(encodeText('a🙂ß?'), {
    ok: false,
    errors: [
      { character: '🙂', position: 1 },
      { character: 'ß', position: 2 },
      { character: '?', position: 3 },
    ],
  });
});

test('one-, three-, and seven-unit gaps replace one another at boundaries', () => {
  assert.deepEqual(toSegments(encoded('AE E')), [
    { signal: 'on', units: 1 },
    { signal: 'off', units: 1 },
    { signal: 'on', units: 3 },
    { signal: 'off', units: 3 },
    { signal: 'on', units: 1 },
    { signal: 'off', units: 7 },
    { signal: 'on', units: 1 },
  ]);
});

test('encoding is lazy and invalid input remains data in the stream', () => {
  let starts = 0;
  function input() {
    starts += 1;
    return of('🙂', 'ok');
  }
  const source = defer(input).pipe(encodeMorse());
  assert.equal(starts, 0);
  const values = [];
  let completed = false;
  source.subscribe({
    next(value) { values.push(value); },
    complete() { completed = true; },
  });
  assert.equal(starts, 1);
  assert.equal(values[0].ok, false);
  assert.equal(formatMorse(values[1].value), '--- -.-');
  assert.equal(completed, true);
});

test('SOS signals have exact virtual times, including final word silence', () => {
  const clock = makeClock();
  const { events } = observe(playMorse(encoded('SOS'), 10, clock), clock);
  clock.flush();
  assert.deepEqual(events, [
    [0, 'on'], [10, 'off'], [20, 'on'], [30, 'off'],
    [40, 'on'], [50, 'off'],
    [80, 'on'], [110, 'off'], [120, 'on'], [150, 'off'],
    [160, 'on'], [190, 'off'],
    [220, 'on'], [230, 'off'], [240, 'on'], [250, 'off'],
    [260, 'on'], [270, 'off'], [340, 'complete'],
  ]);
});

test('a word boundary is exactly seven units of silence', () => {
  const clock = makeClock();
  const { events } = observe(playMorse(encoded('E E'), 10, clock), clock);
  clock.flush();
  assert.deepEqual(events, [
    [0, 'on'], [10, 'off'], [80, 'on'], [90, 'off'], [160, 'complete'],
  ]);
});

test('unsubscription cancels pending playback; the sink resets in finalize', () => {
  const clock = makeClock();
  const resets = [];
  function resetSink() { resets.push([clock.now(), 'off']); }
  const source = playMorse(encoded('SOS'), 10, clock).pipe(finalize(resetSink));
  const { events, subscription } = observe(source, clock);
  function stop() { subscription.unsubscribe(); }
  clock.schedule(stop, 25);
  clock.flush();
  assert.deepEqual(events, [[0, 'on'], [10, 'off'], [20, 'on']]);
  assert.deepEqual(resets, [[25, 'off']]);
});

test('playback is cold: subscriptions have independent timelines', () => {
  const clock = makeClock();
  const source = playMorse(encoded('E'), 10, clock);
  assert.equal(clock.actions.length, 0);
  const first = observe(source, clock);
  let second;
  function startSecond() { second = observe(source, clock); }
  clock.schedule(startSecond, 5);
  clock.flush();
  assert.deepEqual(first.events, [[0, 'on'], [10, 'off'], [80, 'complete']]);
  assert.deepEqual(second.events, [[5, 'on'], [15, 'off'], [85, 'complete']]);
});

test('concatMap queues whole messages with seven-unit separation', () => {
  const clock = makeClock();
  function playEncoding(result) {
    assert.equal(result.ok, true);
    return playMorse(result.value, 10, clock);
  }
  const source = of('E', 'T').pipe(encodeMorse(), concatMap(playEncoding));
  const { events } = observe(source, clock);
  clock.flush();
  assert.deepEqual(events, [
    [0, 'on'], [10, 'off'], [80, 'on'], [110, 'off'], [180, 'complete'],
  ]);
});

test('empty playback switches off and completes without scheduling a timer', () => {
  const clock = makeClock();
  const { events } = observe(playMorse(encoded(''), 10, clock), clock);
  assert.deepEqual(events, [[0, 'off'], [0, 'complete']]);
  assert.equal(clock.actions.length, 0);
});

test('invalid timing is reported on subscription through the error channel', () => {
  for (const unitMs of [0, -1, NaN, Infinity, 2_147_483_647]) {
    const clock = makeClock();
    const source = playMorse(encoded('E'), unitMs, clock);
    const { events } = observe(source, clock);
    assert.equal(events.length, 1);
    assert.equal(events[0][0], 0);
    assert.ok(events[0][1] instanceof RangeError);
    assert.equal(clock.actions.length, 0);
  }
});
