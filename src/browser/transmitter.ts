import {
  concat, concatMap, defer, filter, fromEvent, map, merge, of, scan,
  startWith, switchMap,
} from 'rxjs';
import type { Observable, SchedulerLike } from 'rxjs';
import { encodeText, formatMorse, playMorse } from '../encoder.js';
import type {
  EncodeResult, MorseMessage, Signal, UnsupportedCharacter,
} from '../encoder.js';
import { initialTranscript, reduceTranscript } from '../morse-decoder.js';
import type { MorseEvent, Transcript } from '../morse-decoder.js';
import { decodeSignals } from '../transceiver.js';
import { requireElement } from './dom.js';

type ValidEncoding = Extract<EncodeResult, { ok: true }>;
type SessionAction = 'cancel' | 'reset';

function isSendKey(event: KeyboardEvent): boolean {
  return event.key === 'Enter' && !event.repeat && !event.isComposing
    && !event.altKey && !event.ctrlKey && !event.metaKey && !event.shiftKey;
}

function isReady(result: EncodeResult): result is ValidEncoding {
  return result.ok && result.value.length > 0;
}

function getMessage(result: ValidEncoding): MorseMessage {
  return result.value;
}

function describeProblem(problem: UnsupportedCharacter): string {
  return `${JSON.stringify(problem.character)} at character ${problem.position + 1}`;
}

function stopRequested(): SessionAction { return 'cancel'; }
function clearRequested(): SessionAction { return 'reset'; }

/** One playback/decoder subscription; DOM effects live in the renderers. */
export function mountTransmitter(unitMs: number, scheduler: SchedulerLike): () => void {
  const input = requireElement<HTMLInputElement>('#message');
  const preview = requireElement<HTMLOutputElement>('#morse');
  const send = requireElement<HTMLButtonElement>('#send');
  const stop = requireElement<HTMLButtonElement>('#stop');
  const clear = requireElement<HTMLButtonElement>('#clear-transmission');
  const lamp = requireElement<HTMLElement>('#lamp');
  const status = requireElement<HTMLOutputElement>('#transmit-status');
  const received = requireElement<HTMLOutputElement>('#received');
  const code = requireElement<HTMLOutputElement>('#received-code');
  const issue = requireElement<HTMLOutputElement>('#transmit-issue');

  function readText(): string { return input.value; }

  function inputValues(): Observable<string> {
    return fromEvent(input, 'input').pipe(map(readText), startWith(readText()));
  }

  function renderEncoding(result: EncodeResult): void {
    preview.textContent = result.ok
      ? formatMorse(result.value) || 'Type a message to see its Morse code.'
      : `Unsupported input: ${result.errors.map(describeProblem).join(', ')}`;
    preview.dataset.kind = result.ok ? 'code' : 'error';
    input.setAttribute('aria-invalid', String(!result.ok));
    send.disabled = !isReady(result);
  }

  function playMessage(message: MorseMessage): Observable<Signal> {
    return playMorse(message, unitMs, scheduler);
  }

  const enter$ = fromEvent<KeyboardEvent>(input, 'keydown').pipe(filter(isSendKey));
  // Snapshot the input before concatMap queues it; edits cannot change the queue.
  const messages$ = merge(enter$, fromEvent(send, 'click')).pipe(
    map(readText), map(encodeText), filter(isReady), map(getMessage),
  );

  function startSession(action: SessionAction): Observable<MorseEvent> {
    const signal$ = messages$.pipe(concatMap(playMessage));
    return concat(
      of<MorseEvent>({ kind: action, at: scheduler.now() }),
      signal$.pipe(decodeSignals(unitMs, scheduler)),
    );
  }

  const state$ = merge(
    fromEvent(stop, 'click').pipe(map(stopRequested)),
    fromEvent(clear, 'click').pipe(map(clearRequested)),
  ).pipe(
    startWith<SessionAction>('reset'),
    switchMap(startSession), // Cancel playback, queued messages, and idle checks.
    scan(reduceTranscript, initialTranscript),
  );

  function renderTransmission(state: Transcript): void {
    lamp.dataset.signal = state.pressed ? 'on' : 'off';
    received.textContent = state.text || 'The receiver is waiting for a signal.';
    code.textContent = state.currentCode || '—';
    issue.textContent = state.issue ?? '';
    status.textContent = state.pressed ? 'Signal on'
      : state.currentCode !== '' ? 'Signal off · listening to the pause'
        : 'Signal off';
  }

  const previewSubscription = defer(inputValues).pipe(map(encodeText)).subscribe(renderEncoding);
  const transmissionSubscription = state$.subscribe(renderTransmission);

  function dispose(): void {
    previewSubscription.unsubscribe();
    transmissionSubscription.unsubscribe();
    lamp.dataset.signal = 'off';
    status.textContent = 'Signal off';
  }
  return dispose;
}
