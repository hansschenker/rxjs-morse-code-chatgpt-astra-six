import assert from 'node:assert/strict';
import test from 'node:test';
import { TestScheduler } from 'rxjs/testing';
import { mountReceiver } from '../dist/browser/receiver.js';
import { mountTransmitter } from '../dist/browser/transmitter.js';

// Minimal DOM boundary: real EventTargets, with no fake RxJS operators or timers.
// These tests check adapter behavior, not browser layout or pointer-capture APIs.
function eventTarget() {
  const target = new EventTarget();
  const add = target.addEventListener.bind(target);
  const remove = target.removeEventListener.bind(target);
  target.listeners = 0;
  target.addEventListener = function listen(...args) { target.listeners++; add(...args); };
  target.removeEventListener = function unlisten(...args) { target.listeners--; remove(...args); };
  return target;
}

function fire(target, type, values = {}) {
  const event = Object.assign(new Event(type, { cancelable: true }), values);
  target.dispatchEvent(event);
  return event;
}

function makePage(t) {
  const oldWindow = globalThis.window;
  const oldDocument = globalThis.document;
  const elements = new Map();
  const scheduler = new TestScheduler(assert.deepEqual);
  const window = eventTarget();
  let focused;

  function element(id) {
    if (!elements.has(id)) {
      const node = Object.assign(eventTarget(), {
        value: '', textContent: '', dataset: {}, attributes: {}, disabled: false,
        setAttribute(name, value) { this.attributes[name] = value; },
        setPointerCapture() {},
        focus() {
          if (focused !== this) {
            if (focused) fire(focused, 'blur');
            focused = this;
          }
        },
      });
      elements.set(id, node);
    }
    return elements.get(id);
  }

  function advance(at) {
    // A no-op ensures now() advances even when no timer is pending.
    scheduler.schedule(function advanceClock() {}, at - scheduler.now());
    scheduler.maxFrames = at;
    scheduler.flush();
  }

  function down(repeat = false) {
    return fire(element('#morse-key'), 'keydown', { code: 'Space', key: ' ', repeat });
  }
  function up() { return fire(window, 'keyup', { code: 'Space', key: ' ' }); }
  function pointer(type) {
    return fire(element('#morse-key'), type, { isPrimary: true, button: 0, pointerId: 1 });
  }

  globalThis.window = window;
  globalThis.document = { querySelector: element };
  const disposeTransmitter = mountTransmitter(10, scheduler);
  const disposeReceiver = mountReceiver(10, scheduler);
  function dispose() { disposeTransmitter(); disposeReceiver(); }
  t.after(function restoreGlobals() {
    dispose();
    if (oldWindow === undefined) delete globalThis.window;
    else globalThis.window = oldWindow;
    if (oldDocument === undefined) delete globalThis.document;
    else globalThis.document = oldDocument;
  });
  return { element, window, elements, scheduler, advance, down, up, pointer, dispose };
}

test('combined page previews text, queues snapshots, and receives timed signals', t => {
  const { element, advance } = makePage(t);
  const input = element('#message');
  input.value = 'E';
  fire(input, 'input');
  assert.equal(element('#morse').textContent, '.');
  assert.equal(element('#send').disabled, false);
  fire(input, 'keydown', { key: 'Enter' });
  assert.equal(element('#lamp').dataset.signal, 'on');

  advance(5);
  input.value = 'T';
  fire(element('#send'), 'click');
  input.value = 'SOS'; // The queued message must remain T.
  advance(10);
  assert.equal(element('#received-code').textContent, '.');
  assert.equal(element('#lamp').dataset.signal, 'off');
  advance(40);
  assert.equal(element('#received').textContent, 'E');
  advance(80);
  assert.equal(element('#lamp').dataset.signal, 'on');
  advance(180);
  assert.equal(element('#received').textContent, 'E T');
  assert.equal(element('#lamp').dataset.signal, 'off');
  assert.equal(element('#decoded').dataset.empty, 'true');
});

test('Stop cancels the active message and queue, preserves text, and accepts a fresh send', t => {
  const { element, advance } = makePage(t);
  const input = element('#message');
  input.value = 'E';
  fire(element('#send'), 'click');
  advance(80);
  input.value = 'SOS';
  fire(element('#send'), 'click');
  input.value = 'T';
  fire(element('#send'), 'click');
  advance(85);
  fire(element('#stop'), 'click');
  assert.equal(element('#lamp').dataset.signal, 'off');
  advance(500);
  assert.equal(element('#received').textContent, 'E');
  assert.equal(element('#received-code').textContent, '—');
  fire(element('#send'), 'click');
  advance(600);
  assert.equal(element('#received').textContent, 'E T');
  fire(element('#clear-transmission'), 'click');
  assert.equal(element('#received').textContent, 'The receiver is waiting for a signal.');
});

test('invalid input does not play and text-field Space does not operate the hand key', t => {
  const { element, window, advance } = makePage(t);
  const input = element('#message');
  input.value = 'E?';
  fire(input, 'input');
  assert.equal(element('#send').disabled, true);
  assert.equal(input.attributes['aria-invalid'], 'true');
  fire(input, 'keydown', { key: 'Enter' });
  const down = fire(input, 'keydown', { key: ' ', code: 'Space' });
  const up = fire(window, 'keyup', { key: ' ', code: 'Space' });
  assert.equal(down.defaultPrevented, false);
  assert.equal(up.defaultPrevented, false);
  advance(100);
  assert.equal(element('#morse-key').dataset.pressed, 'false');
  assert.equal(element('#decoded').dataset.empty, 'true');
  assert.equal(element('#lamp').dataset.signal, 'off');
});

test('hand key measures Space duration, ignores repeat, and recognizes A after its pause', t => {
  const { element, down, up, advance } = makePage(t);
  assert.equal(down().defaultPrevented, true);
  advance(5);
  down(true);
  advance(10);
  up();
  advance(20);
  down();
  advance(50);
  up();
  assert.equal(element('#current-code').textContent, '.-');
  assert.equal(element('#last-mark').textContent, 'Dash · 30 ms');
  advance(80);
  assert.equal(element('#decoded').textContent, 'A');
  advance(120);
  assert.match(element('#status').textContent, /Word pause recognized/);
});

test('keyboard and pointer holds combine without producing a premature release', t => {
  const { element, down, up, pointer, advance } = makePage(t);
  down();
  advance(5);
  pointer('pointerdown');
  advance(10);
  up();
  assert.equal(element('#morse-key').dataset.pressed, 'true');
  advance(30);
  pointer('pointerup');
  fire(element('#morse-key'), 'lostpointercapture');
  advance(60);
  assert.equal(element('#decoded').textContent, 'T');
  assert.equal(element('#last-mark').textContent, 'Dash · 30 ms');
});

test('window blur and lost keyboard focus discard a held mark; Clear ignores subsequent repeat', t => {
  const { element, window, down, up, advance } = makePage(t);
  down();
  advance(5);
  fire(window, 'blur');
  advance(10);
  up();
  assert.equal(element('#current-code').textContent, 'Waiting for marks');
  down();
  advance(15);
  fire(element('#morse-key'), 'blur');
  advance(20);
  up();
  down();
  advance(25);
  fire(element('#clear'), 'click');
  advance(30);
  down(true);
  advance(35);
  up();
  advance(150);
  assert.equal(element('#decoded').dataset.empty, 'true');
  assert.equal(element('#morse-key').dataset.pressed, 'false');
});

test('disposing both panels removes every listener and cancels both timelines', t => {
  const { element, window, elements, scheduler, down, advance, dispose } = makePage(t);
  element('#message').value = 'SOS';
  fire(element('#send'), 'click');
  down();
  advance(5);
  dispose();
  assert.equal(element('#lamp').dataset.signal, 'off');
  assert.equal(element('#morse-key').dataset.pressed, 'false');
  assert.equal(window.listeners, 0);
  for (const node of elements.values()) assert.equal(node.listeners, 0);
  assert.equal(scheduler.actions.length, 0);
  fire(element('#send'), 'click');
  advance(500);
  assert.equal(element('#lamp').dataset.signal, 'off');
});
