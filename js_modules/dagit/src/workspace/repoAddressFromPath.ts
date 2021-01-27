import {memoRepoAddress} from 'src/workspace/memoRepoAddress';
import {RepoAddress} from 'src/workspace/types';

export const repoAddressFromPath = (path: string): RepoAddress | null => {
  const postSplit = path.split('@');
  if (postSplit.length === 2) {
    const [name, location] = postSplit;
    return memoRepoAddress(name, location);
  }
  return null;
};
