import * as React from "react";
import { Callout, Intent, Code } from "@blueprintjs/core";

export const SchedulerNotConfigured = () => (
  <Callout
    icon="calendar"
    intent={Intent.WARNING}
    title="The current dagster instance does not have a scheduler configured."
    style={{ marginBottom: 40 }}
  >
    <p>
      A scheduler must be configured on the instance to run schedules. Therefore, the schedules
      below are not currently running. You can configure a scheduler on the instance through the{" "}
      <Code>dagster.yaml</Code> file in <Code>$DAGSTER_HOME</Code>
    </p>

    <p>
      See the{" "}
      <a href="https://docs.dagster.io/overview/instances/dagster-instance#instance-configuration-yaml">
        instance configuration documentation
      </a>{" "}
      for more information.
    </p>
  </Callout>
);
