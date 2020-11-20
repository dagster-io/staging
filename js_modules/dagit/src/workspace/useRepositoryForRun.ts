import {RunFragmentForRepositoryMatch} from 'src/runs/types/RunFragmentForRepositoryMatch';
import {useRepositoryOptions} from 'src/workspace/WorkspaceContext';
import {findRepoContainingPipeline} from 'src/workspace/findRepoContainingPipeline';

/**
 * Given a Run fragment, find the repository that contains its pipeline.
 *
 * - If we have a `RepositoryOrigin`, we can just that to find a match among the repositories in
 *   our workspace.
 * - If it's an old Run with no origin information, try to find its pipeline name among the
 *   repositories in our workspace. This is an imperfect fallback, noted below.
 */
export const useRepositoryForRun = (run: RunFragmentForRepositoryMatch | null | undefined) => {
  const {options} = useRepositoryOptions();
  const origin = run?.repositoryOrigin;
  const location = origin?.repositoryLocationName;
  const name = origin?.repositoryName;

  const match = options.find(
    (option) => option.repository.name === name && option.repositoryLocation.name === location,
  );

  if (match) {
    return match;
  }

  // Fallback: find the first repository that contains the specified pipeline name. This is not ideal,
  // and once we reach a point where all PipelineRun objects should have a repositoryOrigin, we should
  // try to remove this.
  if (run?.pipeline) {
    const possibleMatches = findRepoContainingPipeline(options, run.pipeline.name);
    if (possibleMatches.length) {
      return possibleMatches[0];
    }
  }

  return null;
};
