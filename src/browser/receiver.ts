import {
  concatMap, filter, fromEvent, map, merge, scan,
  shareReplay, startWith,
} from 'rxjs';
import type { SchedulerLike } from 'rxjs';
import { requireElement } from './dom.js';
import {
  decodeMorse, initialTranscript, reduceTranscript,
} from '../morse-decoder.js';
import type { KeyAction, Transcript } from '../morse-decoder.js';

type ControlInput =
  | Readonly<{ kind: 'keyboard' | 'pointer'; down: boolean }>
  | Readonly<{ kind: 'pointer-abort' }>
  | Readonly<{ kind: 'keyboard-abort' }>
  | Readonly<{ kind: 'blur' }>
  | Readonly<{ kind: 'reset' }>
  | Readonly<{ kind: 'repeat' }>;

type Controls = Readonly<{
  keyboard: boolean;
  pointer: boolean;
  actions: readonly KeyAction[];
}>;

const initialControls: Controls = Object.freeze({ keyboard: false, pointer: false, actions: [] });

// A keyboard key and a pointer can hold the same logical key. Releasing one
// must not release it while the other is still held. This adapter state is pure.
function reduceControls(previous: Controls, input: ControlInput): Controls {
  const state: Controls = { ...previous, actions: [] };
  if (input.kind === 'repeat') return state;
  if (input.kind === 'reset') return { ...initialControls, actions: ['reset'] };
  if (input.kind === 'blur') {
    return state.keyboard || state.pointer
      ? { ...initialControls, actions: ['cancel'] }
      : state;
  }
  if (input.kind === 'keyboard-abort') {
    return state.keyboard
      ? { ...state, keyboard: false, actions: state.pointer ? [] : ['cancel'] }
      : state;
  }
  if (input.kind === 'pointer-abort') {
    return state.pointer
      ? { ...state, pointer: false, actions: state.keyboard ? [] : ['cancel'] }
      : state;
  }
  const wasDown = state.keyboard || state.pointer;
  const next = { ...state, [input.kind]: input.down };
  const isDown = next.keyboard || next.pointer;
  return { ...next, actions: wasDown === isDown ? [] : [isDown ? 'press' : 'release'] };
}

function controlActions(state: Controls): readonly KeyAction[] {
  return state.actions;
}

function isSpace(event: KeyboardEvent): boolean {
  return event.code === 'Space';
}

// Event normalization is the DOM boundary. Preventing the browser's default
// Space scrolling belongs here; the decoder and its reducers have no DOM access.
function keyboardDown(event: KeyboardEvent): ControlInput {
  event.preventDefault();
  return event.repeat ? { kind: 'repeat' } : { kind: 'keyboard', down: true };
}

function keyboardUp(): ControlInput {
  return { kind: 'keyboard', down: false };
}

function isPrimaryPointer(event: PointerEvent): boolean {
  return event.isPrimary && event.button === 0;
}

function pointerUp(): ControlInput {
  return { kind: 'pointer', down: false };
}

function pointerAborted(): ControlInput {
  return { kind: 'pointer-abort' };
}

function blurred(): ControlInput {
  return { kind: 'blur' };
}

function resetRequested(): ControlInput {
  return { kind: 'reset' };
}

/** Mount explicitly; dispose removes listeners, cancels timers, and releases UI. */
export function mountReceiver(unitMs: number, scheduler: SchedulerLike): () => void {
  const key = requireElement<HTMLButtonElement>('#morse-key');
  const clear = requireElement<HTMLButtonElement>('#clear');
  const text = requireElement<HTMLOutputElement>('#decoded');
  const code = requireElement<HTMLOutputElement>('#current-code');
  const lastMark = requireElement<HTMLOutputElement>('#last-mark');
  const status = requireElement<HTMLOutputElement>('#status');
  const error = requireElement<HTMLOutputElement>('#issue');

  function pointerDown(event: PointerEvent): ControlInput {
    event.preventDefault();
    key.focus();
    key.setPointerCapture(event.pointerId);
    return { kind: 'pointer', down: true };
  }

  function acceptsKeyDown(event: KeyboardEvent): boolean {
    return isSpace(event) && !event.isComposing
      && !event.altKey && !event.ctrlKey && !event.metaKey;
  }

  function keyboardAborted(): ControlInput { return { kind: 'keyboard-abort' }; }

  const controlInputs$ = merge(
    fromEvent<KeyboardEvent>(key, 'keydown').pipe(filter(acceptsKeyDown), map(keyboardDown)),
    fromEvent<KeyboardEvent>(window, 'keyup').pipe(filter(isSpace), map(keyboardUp)),
    fromEvent<PointerEvent>(key, 'pointerdown').pipe(filter(isPrimaryPointer), map(pointerDown)),
    fromEvent<PointerEvent>(key, 'pointerup').pipe(filter(isPrimaryPointer), map(pointerUp)),
    fromEvent(key, 'pointercancel').pipe(map(pointerAborted)),
    fromEvent(key, 'lostpointercapture').pipe(map(pointerAborted)),
    fromEvent(window, 'blur').pipe(map(blurred)),
    fromEvent(key, 'blur').pipe(map(keyboardAborted)),
    fromEvent(clear, 'click').pipe(map(resetRequested)),
  );

  const actions$ = controlInputs$.pipe(
    scan(reduceControls, initialControls),
    concatMap(controlActions),
  );

  const state$ = actions$.pipe(
    decodeMorse(unitMs, scheduler),
    scan(reduceTranscript, initialTranscript),
    startWith(initialTranscript),
    shareReplay({ bufferSize: 1, refCount: true }),
  );

  function renderTranscript(state: Transcript): void {
    text.textContent = state.text || 'Your decoded message will appear here.';
    text.dataset.empty = String(state.text === '');
    code.textContent = state.currentCode || 'Waiting for marks';
    lastMark.textContent = state.lastMark === null || state.lastDurationMs === null
      ? 'No mark yet'
      : `${state.lastMark === '.' ? 'Dot' : 'Dash'} · ${Math.round(state.lastDurationMs)} ms`;
    error.textContent = state.issue ?? '';
  }

  function renderKey(state: Transcript): void {
    key.dataset.pressed = String(state.pressed);
    key.setAttribute('aria-pressed', String(state.pressed));
    status.textContent = state.pressed
      ? 'Key down — release to finish the mark'
      : state.currentCode !== ''
        ? 'Listening to the pause…'
        : state.pendingSpace && state.text !== ''
          ? 'Word pause recognized — ready for the next word'
          : 'Ready for a press';
  }

  // Two renderers intentionally share ONE live decoder and its remembered state.
  const transcriptSubscription = state$.subscribe(renderTranscript);
  const keySubscription = state$.subscribe(renderKey);

  function dispose(): void {
    transcriptSubscription.unsubscribe();
    keySubscription.unsubscribe();
    renderKey(initialTranscript);
  }
  return dispose;
}
