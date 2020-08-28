import * as React from "react";
import { useMutation } from "react-apollo";
import { Button } from "@blueprintjs/core";
import gql from "graphql-tag";
import { useRepositorySelector } from "./DagsterRepositoryContext";
import { showCustomAlert } from "./CustomAlertProvider";
import PythonErrorInfo from "./PythonErrorInfo";

const TRIGGER_MUTATION = gql`
  mutation TriggerExecution($triggerSelector: TriggerSelector!) {
    triggerExecution(triggerSelector: $triggerSelector) {
      __typename
      ... on TriggerExecutionSuccess {
        launchedRunIds
      }
      ... on PythonError {
        message
        stack
      }
    }
  }
`;

export const TriggerButton: React.FunctionComponent<{ triggerName: string }> = ({
  triggerName
}) => {
  const repositorySelector = useRepositorySelector();
  const [disabled, setDisabled] = React.useState(false);
  const [triggerExecution] = useMutation(TRIGGER_MUTATION);
  const onTrigger = async () => {
    setDisabled(true);
    const variables = {
      triggerSelector: {
        repositoryName: repositorySelector.repositoryName,
        repositoryLocationName: repositorySelector.repositoryLocationName,
        triggerName
      }
    };
    try {
      const result = await triggerExecution({ variables });

      if (result.data.triggerExecution.__typename === "PythonError") {
        showCustomAlert({
          body: <PythonErrorInfo error={result.data.triggerExecution} />
        });
      }

      console.log(result);
    } catch (error) {
      console.error("Error launching run:", error);
    }
    setDisabled(false);
  };
  return (
    <div style={{ display: "flex", margin: 30, alignItems: "flex-start" }}>
      <Button onClick={onTrigger} disabled={disabled}>
        Trigger Execution
      </Button>
    </div>
  );
};
