{{- define "dagsterYaml.runCoordinator.queued" }}
{{- $queuedRunCoordinatorConfig := .Values.dagsterDaemon.runCoordinator.config.queuedRunCoordinator }}
module: dagster.core.run_coordinator
class: QueuedRunCoordinator
config:
  max_concurrent_runs: {{ $queuedRunCoordinatorConfig.maxConcurrentRuns | default "~" }}

  {{- if $queuedRunCoordinatorConfig.tagConcurrencyLimits }}
  tag_concurrency_limits: {{ $queuedRunCoordinatorConfig.tagConcurrencyLimits | toYaml | nindent 2 }}
  {{- end }}

  dequeue_interval_seconds: {{ $queuedRunCoordinatorConfig.dequeueIntervalSeconds | default "~" }}
{{- end }}

{{- define "dagsterYaml.runCoordinator.custom" }}
{{- $customRunCoordinatorConfig := .Values.dagsterDaemon.runCoordinator.config.customRunCoordinator }}
module: {{ $customRunCoordinatorConfig.module | quote }}
class: {{ $customRunCoordinatorConfig.class | quote }}
config: {{ $customRunCoordinatorConfig.config | toYaml | nindent 2 }}
{{- end }}
