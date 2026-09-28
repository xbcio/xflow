// Repeat row identity (Doc B §3.4).
//
// - `repeat.key` set and the row carries a string/number under it: "k:<value>".
// - otherwise object rows get a uid from a module-wide WeakMap, so identity
//   follows the object reference ("r<n>");
// - scalar rows fall back to their index ("i<n>").
// Duplicates (same key twice, the same object twice) get a "~<n>" suffix.
//
// Because edited rows are new objects under immutable updates, applyPatches
// hands the old row's uid to its copy (adoptRowIdentity); hosts applying
// patches with their own reducer should call adoptRowIdentity likewise.

const objectUids = new WeakMap<object, string>();
let nextUid = 1;

export function objectUid(row: object): string {
  let uid = objectUids.get(row);
  if (!uid) {
    uid = `r${nextUid++}`;
    objectUids.set(row, uid);
  }
  return uid;
}

/** Transfers the row identity of `previous` to its replacement `next`. */
export function adoptRowIdentity(next: object, previous: object): void {
  if (next === previous || objectUids.has(next)) return;
  const uid = objectUids.get(previous);
  if (uid) objectUids.set(next, uid);
}

export function rowUids(rows: readonly unknown[], key?: string): string[] {
  const seen = new Map<string, number>();
  return rows.map((row, index) => {
    let uid: string;
    if (row !== null && typeof row === "object") {
      const keyed = key !== undefined && !Array.isArray(row) ? (row as Record<string, unknown>)[key] : undefined;
      uid = typeof keyed === "string" || typeof keyed === "number" ? `k:${String(keyed)}` : objectUid(row);
    } else {
      uid = `i${index}`;
    }
    const count = seen.get(uid) ?? 0;
    seen.set(uid, count + 1);
    return count === 0 ? uid : `${uid}~${count}`;
  });
}
