// A tree describes the alphabet. A path of dot/dash edges is a character's code.
export type Mark = '.' | '-';
export type MorseCode = readonly Mark[];

export type MorseTree = Readonly<{
  character: string | null;
  dot: MorseTree;
  dash: MorseTree;
}> | null;

function branch(
  character: string | null,
  dot: MorseTree = null,
  dash: MorseTree = null,
): MorseTree {
  return Object.freeze({ character, dot, dash });
}

// International Morse: A-Z and 0-9. Null labels are paths without a character.
export const morseTree: MorseTree = branch(null,
  branch('E',
    branch('I',
      branch('S',
        branch('H', branch('5'), branch('4')),
        branch('V', null, branch('3'))),
      branch('U',
        branch('F'),
        branch(null, null, branch('2')))),
    branch('A',
      branch('R', branch('L')),
      branch('W', branch('P'), branch('J', null, branch('1'))))),
  branch('T',
    branch('N',
      branch('D', branch('B', branch('6')), branch('X')),
      branch('K', branch('C'), branch('Y'))),
    branch('M',
      branch('G', branch('Z', branch('7')), branch('Q')),
      branch('O',
        branch(null, branch('8')),
        branch(null, branch('9'), branch('0'))))),
);

// The generalized fold replaces empty trees and branches with supplied values.
export function foldTree<A>(
  empty: A,
  combine: (character: string | null, dot: A, dash: A) => A,
  tree: MorseTree,
): A {
  return tree === null
    ? empty
    : combine(
        tree.character,
        foldTree(empty, combine, tree.dot),
        foldTree(empty, combine, tree.dash),
      );
}

type Entry = readonly [character: string, code: MorseCode];
type Entries = readonly Entry[];

function prepend(mark: Mark): (entry: Entry) => Entry {
  return function prependToEntry([character, code]: Entry): Entry {
    return [character, Object.freeze([mark, ...code])];
  };
}

const prependDot = prepend('.');
const prependDash = prepend('-');

function collectCodes(
  character: string | null,
  dot: Entries,
  dash: Entries,
): Entries {
  const here: Entries = character === null ? [] : [[character, []]];
  return [...here, ...dot.map(prependDot), ...dash.map(prependDash)];
}

// Compiled once from the tree; no second hand-maintained alphabet table.
const entries = foldTree<Entries>([], collectCodes, morseTree);

export const encodingTable: Readonly<Record<string, MorseCode | undefined>> =
  Object.freeze(Object.fromEntries(entries));

function reverseEntry([letter, code]: Entry): readonly [string, string] {
  return [code.join(''), letter];
}

// Both directions derive A-Z/0-9 from the same folded tree.
export const decodingTable: Readonly<Record<string, string | undefined>> =
  Object.freeze(Object.fromEntries(entries.map(reverseEntry)));

// Additional decoding-only entries retained from the supplied Angular sample.
// Their historical conventions are not all part of the ITU core alphabet.
export const morseAlphabet: Readonly<Record<string, string | undefined>> = Object.freeze({
  ...decodingTable,
  ".--.-": "À",
  ".-.-": "Ä",
  ".-..-": "È",
  "..-..": "É",
  "---.": "Ö",
  "..--": "Ü",
  "...--..": "ß",
  "----": "CH",
  "--.--": "Ñ",
  ".-.-.-": ".",
  "--..--": ",",
  "---...": ":",
  "-.-.-.": "",
  "..--..": "?",
  "-....-": "-",
  "..--.-": "_",
  "-.--.": "(",
  "-.--.-": ")",
  ".----.": "'",
  "-...-": "=",
  ".-.-.": "+",
  "-..-.": "/",
  ".--.-.": "@",
});
