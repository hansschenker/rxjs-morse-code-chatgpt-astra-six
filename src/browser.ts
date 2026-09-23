import { asyncScheduler } from 'rxjs';
import { mountReceiver } from './browser/receiver.js';
import { mountTransmitter } from './browser/transmitter.js';

/** Both panels use one nominal dot unit and the same explicit clock. */
export function mountMorseApp(): () => void {
  const disposeTransmitter = mountTransmitter(150, asyncScheduler);
  const disposeReceiver = mountReceiver(150, asyncScheduler);

  function dispose(): void {
    disposeTransmitter();
    disposeReceiver();
  }
  return dispose;
}

// The standalone page owns this lifetime. Embedding apps can call the mounts
// directly and dispose them when their view is removed.
mountMorseApp();
