import assert from 'node:assert/strict';
import test from 'node:test';
import { Observable, defer, of, scan, shareReplay, startWith } from 'rxjs';
import { TestScheduler } from 'rxjs/testing';
import {
  decodeMorse, decodeMorseWith, decodeSymbol, initialTranscript, reduceTranscript,
} from '../dist/morse-decoder.js';
import { decodeSignals, encodeText, playMorse } from '../dist/index.js';

const alphabet = {
  A: '.-', B: '-...', C: '-.-.', D: '-..', E: '.', F: '..-.',
  G: '--.', H: '....', I: '..', J: '.---', K: '-.-', L: '.-..',
  M: '--', N: '-.', O: '---', P: '.--.', Q: '--.-', R: '.-.',
  S: '...', T: '-', U: '..-', V: '...-', W: '.--', X: '-..-',
  Y: '-.--', Z: '--..',
  0: '-----', 1: '.----', 2: '..---', 3: '...--', 4: '....-',
  5: '.....', 6: '-....', 7: '--...', 8: '---..', 9: '----.',
};

function clock() {
  const scheduler = new TestScheduler(assert.deepEqual);
  scheduler.maxFrames = 100_000;
  return scheduler;
}

function keySource(scheduler, steps, completeAt = steps.at(-1)?.[0] ?? 0, counts = { starts: 0, active: 0 }) {
  return new Observable(function connect(subscriber) {
    counts.starts++;
    counts.active++;
    const tasks = steps.map(function scheduleKey([at, action]) {
      return scheduler.schedule(function emitKey() { subscriber.next(action); }, at);
    });
    if (completeAt !== null) {
      tasks.push(scheduler.schedule(function finish() { subscriber.complete(); }, completeAt));
    }
    return function disconnect() {
      counts.active--;
      tasks.forEach(function cancel(task) { task.unsubscribe(); });
    };
  });
}

function observe(source, scheduler) {
  const events = [];
  const completed = [];
  const errors = [];
  const subscription = source.subscribe({
    next(event) { events.push(event); },
    error(error) { errors.push(error); },
    complete() { completed.push(scheduler.now()); },
  });
  return { events, completed, errors, subscription };
}

function finalText(events) {
  return events.reduce(reduceTranscript, initialTranscript).text;
}

function letters(events) {
  return events.filter(event => event.kind === 'letter').map(event => [event.at, event.letter]);
}

function actionsForCodes(words, unit = 10) {
  let time = 0;
  const steps = [];
  words.forEach((word, wordIndex) => {
    if (wordIndex > 0) time += 7 * unit;
    word.forEach((code, letterIndex) => {
      if (letterIndex > 0) time += 3 * unit;
      Array.from(code).forEach((mark, markIndex) => {
        if (markIndex > 0) time += unit;
        steps.push([time, 'press']);
        time += (mark === '.' ? 1 : 3) * unit;
        steps.push([time, 'release']);
      });
    });
  });
  return steps;
}

test('decodes the independent A-Z and 0-9 fixture from timed key input', () => {
  for (const [letter, code] of Object.entries(alphabet)) {
    const scheduler = clock();
    const input = keySource(scheduler, actionsForCodes([[code]]));
    const result = observe(input.pipe(decodeMorse(10, scheduler)), scheduler);
    scheduler.flush();
    assert.equal(finalText(result.events), letter, code);
    assert.deepEqual(result.errors, []);
  }
});

test('retains extended mappings from the supplied Angular alphabet', () => {
  assert.deepEqual(decodeSymbol('.-.-'), { ok: true, code: '.-.-', letter: 'Ä' });
  assert.deepEqual(decodeSymbol('----'), { ok: true, code: '----', letter: 'CH' });
  assert.deepEqual(decodeSymbol('...--..'), { ok: true, code: '...--..', letter: 'ß' });
  assert.deepEqual(decodeSymbol('.----.'), { ok: true, code: '.----.', letter: "'" });
});

test('unknown, empty, inherited-property, and unassigned codes are data errors', () => {
  for (const code of ['........', '', 'toString', '__proto__', '-.-.-.']) {
    assert.deepEqual(decodeSymbol(code), { ok: false, code });
  }
});

test('recognizes letters and word boundaries at exact virtual times', () => {
  const scheduler = clock();
  const source = keySource(scheduler, actionsForCodes([['.-', '.'], ['.']]));
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[80, 'A'], [120, 'E'], [200, 'E']]);
  assert.deepEqual(result.events.filter(e => e.kind === 'word-gap').map(e => e.at), [160, 240]);
  assert.equal(finalText(result.events), 'AE E');
  assert.deepEqual(result.completed, [240]);
});

test('the inclusive dot threshold is distinct from silence thresholds', () => {
  const scheduler = clock();
  const source = keySource(scheduler, [[0, 'press'], [20, 'release'], [30, 'press'], [51, 'release']]);
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(result.events.filter(e => e.kind === 'mark').map(e => [e.mark, e.durationMs]), [['.', 20], ['-', 21]]);
  assert.equal(finalText(result.events), 'A');
});

test('explicit thresholds give one coherent letter and word silence policy', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [100, 'release']])
    .pipe(decodeMorseWith(200, 450, 1000, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[550, 'E']]);
  assert.deepEqual(result.events.filter(e => e.kind === 'word-gap').map(e => e.at), [1100]);
});

test('a press just before a letter deadline cancels that deadline', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [10, 'release'], [39, 'press'], [49, 'release']])
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[79, 'I']]);
});

test('exact boundary equality is stable when the source event runs before the timer', () => {
  for (const [secondPress, expected] of [[40, 'EE'], [80, 'E E']]) {
    const scheduler = clock();
    const source = keySource(scheduler, [[0, 'press'], [10, 'release'], [secondPress, 'press'], [secondPress + 10, 'release']]);
    const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
    scheduler.flush();
    assert.equal(finalText(result.events), expected);
    assert.equal(letters(result.events).length, 2);
  }
});

test('exact boundary equality also works when the timer is registered first', () => {
  for (const [secondPress, expected] of [[40, 'EE'], [80, 'E E']]) {
    const scheduler = clock();
    const source = new Observable(subscriber => {
      subscriber.next('press');
      const release = scheduler.schedule(() => {
        subscriber.next('release');
        // The decoder registered its silence timers during this next call.
        scheduler.schedule(() => subscriber.next('press'), secondPress - 10);
        scheduler.schedule(() => { subscriber.next('release'); subscriber.complete(); }, secondPress);
      }, 10);
      return () => release.unsubscribe();
    });
    const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
    scheduler.flush();
    assert.equal(finalText(result.events), expected);
    assert.equal(letters(result.events).length, 2);
  }
});

test('no silence is recognized while the key is held, even for a long dash', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [10, 'release'], [20, 'press'], [200, 'release']])
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[230, 'A']]);
});

test('duplicate actions do not restart a press or postpone silence deadlines', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [5, 'press'], [10, 'release'], [20, 'release']])
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(result.events.filter(e => e.kind === 'mark').map(e => e.durationMs), [10]);
  assert.deepEqual(letters(result.events), [[40, 'E']]);
  assert.deepEqual(result.completed, [80]);
});

test('orphan releases are ignored without starting silence timers', () => {
  const scheduler = clock();
  const result = observe(of('release').pipe(decodeMorse(10, scheduler)), scheduler);
  assert.deepEqual(result.events, []);
  assert.deepEqual(result.completed, [0]);
  assert.equal(scheduler.actions.length, 0);
});

test('continued silence produces only one letter and one word boundary', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [10, 'release']], 500)
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[40, 'E']]);
  assert.equal(result.events.filter(e => e.kind === 'word-gap').length, 1);
  assert.equal(finalText(result.events), 'E');
});

test('unknown sequences do not terminate decoding of later letters', () => {
  const scheduler = clock();
  const source = keySource(scheduler, actionsForCodes([['........', '.']]));
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.equal(result.events.filter(e => e.kind === 'invalid-code').length, 1);
  assert.equal(finalText(result.events), '\uFFFDE');
  assert.deepEqual(result.errors, []);
});

test('reset cancels pending recognition and starts with empty memory', () => {
  const scheduler = clock();
  const source = keySource(scheduler, [[0, 'press'], [10, 'release'], [20, 'reset'], [25, 'release'], [30, 'press'], [60, 'release']]);
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(letters(result.events), [[90, 'T']]);
  assert.equal(finalText(result.events), 'T');
});

test('cancel discards an unfinished letter and preserves decoded text', () => {
  const scheduler = clock();
  const source = keySource(scheduler, [[0, 'press'], [10, 'release'], [50, 'press'], [60, 'cancel'], [70, 'release'], [100, 'press'], [130, 'release']]);
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.equal(finalText(result.events), 'E T');
  assert.equal(result.events.filter(e => e.kind === 'mark').length, 2);
});

test('unsubscription cancels timers and emits no synthetic completion boundary', () => {
  const scheduler = clock();
  const counts = { starts: 0, active: 0 };
  const result = observe(keySource(scheduler, [[0, 'press'], [10, 'release']], null, counts)
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.schedule(() => result.subscription.unsubscribe(), 15);
  scheduler.flush();
  assert.deepEqual(letters(result.events), []);
  assert.deepEqual(result.completed, []);
  assert.equal(counts.active, 0);
  assert.equal(scheduler.actions.length, 0);
});

test('normal completion waits for final silence; an unfinished press is reported', () => {
  const scheduler = clock();
  const result = observe(keySource(scheduler, [[0, 'press'], [10, 'release'], [20, 'press']], 25)
    .pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(result.events.at(-1), { kind: 'input-error', at: 25, code: '.', reason: 'unfinished-press' });
  assert.deepEqual(result.completed, [25]);
  assert.deepEqual(letters(result.events), []);
});

test('a zero-duration press is an input error and does not become a dot', () => {
  const scheduler = clock();
  const result = observe(of('press', 'release').pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.equal(result.events.filter(e => e.kind === 'input-error').length, 1);
  assert.equal(result.events.filter(e => e.kind === 'mark').length, 0);
  assert.equal(finalText(result.events), '');
});

test('upstream errors propagate and cancel outstanding idle timers', () => {
  const scheduler = clock();
  const failure = new Error('input failed');
  const source = new Observable(subscriber => {
    subscriber.next('press');
    const up = scheduler.schedule(() => subscriber.next('release'), 10);
    const fail = scheduler.schedule(() => subscriber.error(failure), 20);
    return () => { up.unsubscribe(); fail.unsubscribe(); };
  });
  const result = observe(source.pipe(decodeMorse(10, scheduler)), scheduler);
  scheduler.flush();
  assert.deepEqual(result.errors, [failure]);
  assert.deepEqual(letters(result.events), []);
  assert.equal(scheduler.actions.length, 0);
});

test('each unshared decoder subscription has its own input and memory', () => {
  const scheduler = clock();
  const counts = { starts: 0, active: 0 };
  const source = keySource(scheduler, [[0, 'press'], [10, 'release']], 10, counts)
    .pipe(decodeMorse(10, scheduler));
  assert.equal(counts.starts, 0);
  const first = observe(source, scheduler);
  const second = observe(source, scheduler);
  scheduler.flush();
  assert.equal(counts.starts, 2);
  assert.equal(finalText(first.events), 'E');
  assert.equal(finalText(second.events), 'E');
});

test('explicit state replay shares one live decoder and releases it at refCount zero', () => {
  const scheduler = clock();
  const counts = { starts: 0, active: 0 };
  const state = keySource(scheduler, [[0, 'press'], [10, 'release']], null, counts).pipe(
    decodeMorse(10, scheduler), scan(reduceTranscript, initialTranscript),
    startWith(initialTranscript), shareReplay({ bufferSize: 1, refCount: true }),
  );
  const first = observe(state, scheduler);
  const second = observe(state, scheduler);
  scheduler.maxFrames = 15;
  scheduler.flush();
  assert.equal(counts.starts, 1);
  first.subscription.unsubscribe();
  assert.equal(counts.active, 1);
  second.subscription.unsubscribe();
  assert.equal(counts.active, 0);
  assert.equal(scheduler.actions.length, 0);
  const restarted = observe(state, scheduler);
  assert.deepEqual(restarted.events[0], initialTranscript);
  assert.equal(counts.starts, 2);
  restarted.subscription.unsubscribe();
});

test('invalid timing fails on subscription before connecting to input', () => {
  for (const args of [[0, 30, 70], [20, -1, 70], [20, 30, 30], [20, 30, Infinity], [NaN, 30, 70]]) {
    const scheduler = clock();
    let starts = 0;
    const source = defer(() => { starts++; return of('press'); }).pipe(decodeMorseWith(...args, scheduler));
    assert.equal(starts, 0);
    const result = observe(source, scheduler);
    assert.equal(starts, 0);
    assert.equal(result.errors.length, 1);
    assert.ok(result.errors[0] instanceof RangeError);
  }
});

test('empty input completes immediately without producing a word boundary', () => {
  const scheduler = clock();
  const result = observe(of().pipe(decodeMorse(10, scheduler)), scheduler);
  assert.deepEqual(result.events, []);
  assert.deepEqual(result.completed, [0]);
});

test('round trip through the shared public API recovers text from only timed on/off input', () => {
  for (const text of ['SOS', 'MORSE CODE 123', 'AE E', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ 0123456789']) {
    const scheduler = clock();
    const encoded = encodeText(text);
    assert.equal(encoded.ok, true);
    const source = playMorse(encoded.value, 10, scheduler).pipe(
      decodeSignals(10, scheduler),
    );
    const result = observe(source, scheduler);
    scheduler.flush();
    assert.equal(finalText(result.events), text);
    assert.equal(result.events.filter(e => e.kind === 'invalid-code' || e.kind === 'input-error').length, 0);
    assert.deepEqual(result.errors, []);
  }
});
