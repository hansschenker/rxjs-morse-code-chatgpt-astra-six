# RxJS Morse Code — encoder and decoder

The main contributor to this project is ChatGPT-Astra 6.

A single functional TypeScript solution for text encoding, timed Morse playback,
and decoding key presses and pauses. It uses **RxJS 7.8.2**, pure transformations,
immutable reducer state, named functions, and explicit schedulers. The application
code defines no classes or Subjects. There is no Angular dependency.

## Run it

Use Node.js 22 or newer:

```sh
npm ci
npm test
npm start
```

Open **http://127.0.0.1:8080**. The page has two connected demonstrations:

- **Text → signal → text:** enter text and press Enter or Send. The encoder plays
  the message, and a receiver reconstructs it from the timed on/off stream.
  Additional messages are queued. Stop cancels playback, the queue, and pending
  decoding; Clear also clears the received text.
- **Hand key:** focus the on-screen key and hold Space, or hold it with a mouse
  or touch pointer. Release to finish a mark. Pauses finish letters and words.
  Typing in the text field does not operate the hand key.

The two panels have independent receiver sessions. Both use the same decoder and
a 150 ms nominal dot unit. The page uses a visual signal; it needs no microphone.

```sh
npm run demo            # Timed console round trip: SOS HELP
npm run build           # TypeScript and declarations in dist/
npm run build:browser   # Browser bundle and standalone morse-code.html
```

The generated `morse-code.html` includes RxJS and can be opened directly without
a server or internet connection. Generated files and dependencies are excluded
from Git; the build is reproducible from `package-lock.json`.

## The connection

The receiver gets only signal levels and their arrival times. It does not receive
the source text, Morse symbols, or planned durations.

```ts
import { asyncScheduler, scan } from 'rxjs';
import {
  decodeSignals, encodeText, initialTranscript, playMorse, reduceTranscript,
} from './dist/index.js';
import type { Transcript } from './dist/index.js';

function display(state: Transcript): void {
  console.log(state.text);
}

const encoded = encodeText('SOS HELP');

if (encoded.ok) {
  const transcript$ = playMorse(encoded.value, 150, asyncScheduler).pipe(
    decodeSignals(150, asyncScheduler),
    scan(reduceTranscript, initialTranscript),
  );

  // The consumer owns the effect and its lifetime.
  const subscription = transcript$.subscribe(display);
  // Call subscription.unsubscribe() to stop early.
}
```

`src/index.ts` is the shared public entry point. Importing it creates no timers,
DOM listeners, or subscriptions. `decodeSignals` composes a named signal-to-key
adapter with `decodeMorse`; hand input supplies `press`/`release` actions directly.

## One alphabet, two directions

`src/alphabet.ts` represents A–Z and 0–9 as a binary Morse tree. A generalized
`foldTree` derives both encoding and decoding tables from that tree. This follows
the tree-and-fold idea in Heinrich Apfelmus's
[Fun with Morse Code](https://apfelmus.nfshost.com/articles/fun-with-morse-code.html).

Text encoding accepts A–Z, a–z, digits, and whitespace. It uppercases ASCII letters
and collapses whitespace while preserving word boundaries. Unsupported input is
a typed error result containing the original Unicode code-point positions.

The decoder additionally preserves the extended characters and punctuation in the
supplied Angular sample, including `CH`. These are **decoding-only extensions**;
they do not expand the text encoder's contract. The sample's unassigned `-.-.-.`
entry produces an `invalid-code` event. Extended mappings are historical sample
data and are not all claimed to be part of the ITU core alphabet.

## Timing is part of the dataflow

The transmitter follows the standard 1:3:7 spacing in
[ITU-R M.1677-1](https://www.itu.int/rec/R-REC-M.1677-1-200910-I/en).

| Element | Duration | At a 150 ms unit |
| --- | ---: | ---: |
| Dot | 1 unit | 150 ms |
| Dash | 3 units | 450 ms |
| Silence within a letter | 1 unit | 150 ms |
| Silence between letters | 3 units total | 450 ms |
| Silence between words | 7 units total | 1,050 ms |

Boundary gaps replace one another; they are not added together. Playback ends
with a seven-unit silent interval so queued messages remain separate words.

`decodeMorse(unitMs, scheduler)` classifies a press of **at most two units** as a
dot and a longer press as a dash. After a release, the letter and word deadlines
both start from that release: three and seven silent units. A new press cancels
obsolete deadlines. The pure reducer also checks elapsed silence on that press,
so exact-boundary input works regardless of timer/source ordering.

Use `decodeMorseWith(dotMaxMs, letterSilenceMs, wordSilenceMs, scheduler)` for
explicit thresholds. This is a fixed-speed teaching sample. Real browser timers
are best effort; background throttling or irregular human keying can affect the
result. Automatic speed estimation is not implemented.

## Functional core and lifecycle

| Function | Responsibility |
| --- | --- |
| `foldTree` | Derive values from the immutable Morse tree |
| `encodeText`, `formatMorse` | Pure text conversion and formatting |
| `encodeMorse` | Map input text to typed encoding results |
| `toSegments` | Pure signal-duration description |
| `playMorse` | Cold, cancellable signal playback |
| `decodeSymbol`, `classifyMark`, `reduceMorse` | Pure recognition rules |
| `decodeMorse`, `decodeMorseWith` | Timed key-action decoding operators |
| `decodeSignals` | Connect timed playback to the key decoder |
| `reduceTranscript` | Fold decoder events into display state |

- Core operators do not subscribe internally or add implicit sharing. Each
  subscription owns its timers and decoder state; the input owns its producer.
- Unknown codes are data events, so subsequent valid letters still decode.
  The transcript displays `�` for unknown sequences.
- `cancel` discards an unfinished letter and preserves committed text; `reset`
  also clears the transcript. Unsubscription stops immediately without inventing
  a final release or letter. A physical signal sink must reset on teardown.
- Normal completion waits for the final silence checks. Completion with the key
  held reports an input error. Upstream errors propagate and cancel timers.
- The transmitter uses `concatMap` for its queue and `switchMap` to replace a
  cancelled session. Its display uses one subscription. The hand-key display
  explicitly uses `shareReplay({ bufferSize: 1, refCount: true })` so two renderers
  share one live decoder. Mount functions return a disposal function.

See [the decoder notes](docs/decoder.md) for the Angular-to-functions comparison,
event timing, threshold choices, and completion/sharing details.

## Project layout and checks

```text
src/alphabet.ts           Shared tree, fold, and derived lookup tables
src/encoder.ts            Pure encoding and timed playback
src/morse-decoder.ts      Pure reducer and timed decoding
src/transceiver.ts        Signal-to-decoder connection
src/index.ts              Public API
src/demo.ts               Console round trip
src/browser/              DOM adapters and rendering
src/browser.ts            Browser entry point
browser/index.html       Combined demo page
scripts/build-browser.mjs  Reproducible standalone-page builder
test/                    Virtual-time and browser-adapter behavior tests
```

Tests cover an independent A–Z/0–9 fixture, exact mark and gap timing, boundary
ordering, queues, cancellation, completion, errors, subscription ownership, and
round trips through the public API. Browser-adapter tests also exercise actual
event streams with a virtual clock. Dependencies are pinned to RxJS 7.8.2,
TypeScript 5.9.3, and esbuild 0.28.2.

## Credits

Developed with Hans Schenker. The main contributor to this project is
ChatGPT-Astra 6. The encoder's tree/fold approach is inspired by Heinrich
Apfelmus's article; the decoder refactors the Angular sample supplied in the
conversation. Third-party dependency licenses and notices are included in
`THIRD_PARTY_NOTICES.txt`.

Contribution commits use this Git trailer:

```text
Co-authored-by: ChatGPT-Astra 6 <chatgpt-astra-6@openai.com>
```

[GitHub associates co-authors with accounts](https://docs.github.com/en/pull-requests/how-tos/commit-changes/creating-a-commit-with-multiple-authors) using the trailer's email address;
the trailer by itself does not create a GitHub account or guarantee association.
