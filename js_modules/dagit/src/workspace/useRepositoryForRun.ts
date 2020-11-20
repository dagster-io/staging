import {RunFragmentForRepositoryMatch} from 'src/runs/types/RunFragmentForRepositoryMatch';
import {DagsterRepoOption, useRepositoryOptions} from 'src/workspace/WorkspaceContext';
import {findRepoContainingPipeline} from 'src/workspace/findRepoContainingPipeline';

type MatchType = {
  match: DagsterRepoOption;
  type: 'origin' | 'snapshot' | 'pipeline-name';
};

/**
 * Given a Run fragment, find the repository that contains its pipeline.
 *
 * - If we have a `RepositoryOrigin`, we can just that to find a match among the repositories in
 *   our workspace.
 * - If it's an old Run with no origin information, try to find the pipeline among the
 *   repositories in our workspace. This is an imperfect fallback, noted below.
 */
export const useRepositoryForRun = (
  run: RunFragmentForRepositoryMatch | null | undefined,
): MatchType | null => {
  const {options} = useRepositoryOptions();
  const origin = run?.repositoryOrigin;
  const location = origin?.repositoryLocationName;
  const name = origin?.repositoryName;

  const match = options.find(
    (option) => option.repository.name === name && option.repositoryLocation.name === location,
  );

  if (match) {
    return {match, type: 'origin'};
  }

  /**
   * Fallback:
   * - Find the first repository that contains the specified pipeline name and snapshot ID, if any.
   * - If none found, find the first repository that just matches the pipeline name.
   *
   * This is not ideal, and once we reach a point where all PipelineRun objects should have a
   * repositoryOrigin, we should try to remove this.
   */
  if (run?.pipeline) {
    const pipelineName = run.pipeline.name;
    const snapshotId = run?.pipelineSnapshotId;

    if (snapshotId) {
      const snapshotMatches = findRepoContainingPipeline(options, pipelineName, snapshotId);
      if (snapshotMatches) {
        return {match: snapshotMatches[0], type: 'snapshot'};
      }
    }

    const possibleMatches = findRepoContainingPipeline(options, pipelineName);
    if (possibleMatches.length) {
      return {match: possibleMatches[0], type: 'pipeline-name'};
    }
  }

  return null;
};
