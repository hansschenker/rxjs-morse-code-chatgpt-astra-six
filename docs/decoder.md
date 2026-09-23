# Morse decoder — functional TypeScript and RxJS 7

This refactors the supplied Angular `MorseCodeDecoderService` into ordinary
functions and typed dataflow. A human presses and releases a key; the decoder
interprets the duration of those presses and the silence between them.

The core defines no classes, Subjects, internal subscriptions, Angular services,
or DOM operations. RxJS itself still uses its normal RxJS 7 implementation.

See the [project README](../README.md) for installation and the combined browser demo.

## What changed from the Angular service

| Original | Refactored version |
| --- | --- |
| Two timestamp Subjects | One input Observable of typed key actions |
| `combineLatest` and signed time subtraction | Press/release pairing and explicit measured durations |
| Injected threshold objects | Explicit arguments to `decodeMorse` or `decodeMorseWith` |
| `.`, `-`, `+`, `*` control strings | Discriminated mark, letter, word-gap, and issue events |
| `buffer` driven by a second subscription | One pure `scan` reducer that remembers the unfinished letter |
| Four repeated idle markers | Two one-shot silence deadlines, both measured from release |
| One undifferentiated long break | Separate letter and word recognition |
| Exceptions recovered as the string `ERROR` | Unknown sequences returned as `invalid-code` data |
| Constructor-owned subscription | The consumer owns subscription and cancellation |

The 59 decoding entries are retained from the supplied sample, including the
extended characters and `CH`. The 36 core entries now derive from the shared tree in `src/alphabet.ts`; the remaining entries extend that decoding table. Its empty
translation for `-.-.-.` now produces an explicit unknown-code result instead of
silently contributing an empty string. Extended mappings are retained as supplied;
this project does not claim that every extension belongs to the ITU core alphabet.

## Public functions

| Function | Responsibility |
| --- | --- |
| `decodeSymbol(code)` | Pure table lookup returning a typed success/failure value |
| `classifyMark(durationMs, dotMaxMs)` | Pure dot/dash classification |
| `reduceMorse(timing, memory, input)` | Pure state transition, including boundary recognition |
| `decodeMorse(unitMs, scheduler)` | RxJS operator for the unit-based recognition policy |
| `decodeMorseWith(dotMaxMs, letterSilenceMs, wordSilenceMs, scheduler)` | RxJS operator with three explicit thresholds |
| `reduceTranscript(state, event)` | Pure fold from decoder events to display state |

`decodeMorse(150, scheduler)` uses a 300 ms inclusive dot threshold, a 450 ms
letter pause, and a 1,050 ms word pause. Ideal dots and dashes last one and three
units; the two-unit classification threshold lies between them.

To use the older sample's numeric cutoffs explicitly:

```ts
keyActions$.pipe(
  decodeMorseWith(200, 450, 1000, scheduler),
);
```

This is intentionally a coherent revised policy: a letter is recognized after
450 ms of silence and a word after 1,000 ms. The old service used 450 ms when a
new press arrived but waited 1,000 ms to recognize a final letter during idle.

## What flows

The input values are `press`, `release`, `cancel`, and `reset`. **Their arrival
times matter.** `timestamp(scheduler)` observes each accepted action using the
same clock that drives the silence timers. Supplying `of('press', 'release')`
synchronously gives a zero-duration press; it does not represent a dot.

A mark is emitted as soon as a valid release arrives. After release, two timers
are subscribed: one for the letter threshold and one for the word threshold.
They both start at that release; the word timer does not start after the letter
timer. A new action replaces the current inner stream through `switchMap`,
cancelling obsolete deadlines.

`scan` applies the pure `reduceMorse` transition function. A transition may emit
zero or several events; `concatMap(emittedEvents)` emits that small event list in
order. It does not run a timer or delay those events.

For an ideal A with a 150 ms unit:

| Time | Physical/timer event | Decoder output |
| ---: | --- | --- |
| 0 ms | Press | `press` |
| 150 ms | Release | Dot, duration 150 ms |
| 300 ms | Press | `press`; prior idle checks are cancelled |
| 750 ms | Release | Dash, duration 450 ms |
| 1,200 ms | Letter deadline | Letter A, code `.-` |
| 1,800 ms | Word deadline | One `word-gap` |

The reducer also checks elapsed silence when a new press arrives. Therefore,
a press at exactly a letter or word deadline has the same interpretation
whether the source event or timer is delivered first at that time. A delayed
timer is also checked against observed elapsed silence.

## State, errors, cancellation, and sharing

`decodeMorse` is lazy and adds no sharing. Each subscription gets its own decoder
memory and timers. The input source determines whether the producer is cold or
hot; physical keyboard and pointer producers are external and hot.

The browser shares the remembered transcript explicitly:

```ts
const state$ = keyActions$.pipe(
  decodeMorse(150, asyncScheduler),
  scan(reduceTranscript, initialTranscript),
  startWith(initialTranscript),
  shareReplay({ bufferSize: 1, refCount: true }),
);
```

Its two renderers share one live decoder. Removing the last subscriber from this
live stream removes event listeners and cancels timers; a later subscription
starts with fresh state. A completed `shareReplay` stream retains its final cache,
following RxJS 7's normal completion behavior.

- Repeated presses and duplicate releases are ignored. Unmatched releases do
  not start idle timers.
- Unknown codes emit `invalid-code`, and decoding continues. The transcript
  shows a replacement character (`�`) at that position.
- `cancel` discards the unfinished letter and preserves committed text. The
  transcript treats subsequent input as a new word.
- `reset` discards pending work and clears the transcript.
- Normal source completion waits for active final silence checks. Completion
  during a held key reports `unfinished-press` and discards the unfinished letter.
- Unsubscription stops immediately without inventing a final letter or boundary.
- Upstream errors propagate and cancel pending timers. Invalid timing arguments
  are reported on subscription before connecting to the input source.
- Word gaps are emitted once. The text reducer inserts a space only when another
  letter follows, keeping the displayed text free of trailing spaces.

This is a fixed-speed recognition policy. Browser timers are best effort;
virtual time provides reproducible timing experiments. Automatic speed estimation
and adaptive decoding of irregular human keying are outside this sample.
