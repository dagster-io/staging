import {memoRepoAddress} from 'src/workspace/memoRepoAddress';

describe('memoRepoAddress', () => {
  it('returns the exact object for the same arguments', () => {
    const a = memoRepoAddress('foo', 'bar');
    const b = memoRepoAddress('foo', 'bar');
    expect(a).toBe(b);
    const c = memoRepoAddress('derp', 'baz');
    expect(c).not.toBe(a);
  });
});
