{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "dagster.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "dagster.fullname" -}}
{{- $fullnameOverride := empty .Values.global | ternary .Values.fullnameOverride .Values.global.fullnameOverride }}
{{- $nameOverride := empty .Values.global | ternary .Values.nameOverride .Values.global.nameOverride }}

{{- if $fullnameOverride -}}
{{- $fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name $nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

# Image utils
{{- define "image.name" }}
{{- .repository -}}:{{- .tag -}}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dagster.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels
*/}}
{{- define "dagster.labels" -}}
helm.sh/chart: {{ include "dagster.chart" . }}
{{ include "dagster.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "dagster.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dagster.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "dagsterUserDeployments.serviceAccountName" -}}
{{ include "dagster.fullname" . }}
{{- end -}}

{{/*
This environment shared across all User Code containers
*/}}
{{- define "dagsterUserDeployments.sharedEnv" -}}
{{- $dagsterHome := empty .Values.global | ternary .Values.dagsterHome .Values.global.dagsterHome }}
DAGSTER_HOME: {{ $dagsterHome | quote }}
DAGSTER_K8S_PG_PASSWORD_SECRET: "{{ template "dagster.fullname" .}}-postgresql-secret"
DAGSTER_K8S_INSTANCE_CONFIG_MAP: "{{ template "dagster.fullname" .}}-instance"
DAGSTER_K8S_PIPELINE_RUN_NAMESPACE: "{{ .Release.Namespace }}"
DAGSTER_K8S_PIPELINE_RUN_ENV_CONFIGMAP: "{{ template "dagster.fullname" . }}-pipeline-env"
{{- end -}}
