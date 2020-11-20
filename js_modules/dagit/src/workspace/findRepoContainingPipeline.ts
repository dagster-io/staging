import {DagsterRepoOption} from 'src/workspace/WorkspaceContext';

export const findRepoContainingPipeline = (
  options: DagsterRepoOption[],
  pipelineName: string,
  snapshotId?: string,
) => {
  return (options || []).filter((repo) =>
    repo.repository.pipelines.find(
      (pipeline) =>
        pipeline.name === pipelineName &&
        (!snapshotId || snapshotId === pipeline.pipelineSnapshotId),
    ),
  );
};
