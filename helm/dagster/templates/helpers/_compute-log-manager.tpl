{{- define "dagsterYaml.computeLogManager.local" }}
module: dagster.core.storage.local_compute_log_manager
class: LocalComputeLogManager
config:
  base_dir: "{{ .Values.dagsterHome }}/storage"
{{- end }}

{{- define "dagsterYaml.computeLogManager.azure" }}
{{- $azureBlobComputeLogManagerConfig := .Values.computeLogManager.config.azureBlobComputeLogManager }}
module: dagster_azure.blob.compute_log_manager
class: AzureBlobComputeLogManager
config:
  storage_account: {{ $azureBlobComputeLogManagerConfig.storageAccount | quote }}
  container: {{ $azureBlobComputeLogManagerConfig.container | quote }}
  secret_key: {{ $azureBlobComputeLogManagerConfig.secretKey | quote }}
  local_dir: {{ $azureBlobComputeLogManagerConfig.localDir | quote }}
  prefix: {{ $azureBlobComputeLogManagerConfig.prefix | quote }}
{{- end }}

{{- define "dagsterYaml.computeLogManager.gcs" }}
{{- $gcsComputeLogManagerConfig := .Values.computeLogManager.config.gcsComputeLogManager }}
module: dagster_gcp.gcs.compute_log_manager
class: GCSComputeLogManager
config:
  bucket: {{ $gcsComputeLogManagerConfig.bucket | quote }}
  local_dir: {{ $gcsComputeLogManagerConfig.localDir | quote }}
  prefix: {{ $gcsComputeLogManagerConfig.prefix | quote }}
{{- end }}

{{- define "dagsterYaml.computeLogManager.s3" }}
{{- $s3ComputeLogManagerConfig := .Values.computeLogManager.config.s3ComputeLogManager }}
module: dagster_aws.s3.compute_log_manager
class: S3ComputeLogManager
config:
  bucket: {{ $s3ComputeLogManagerConfig.bucket | quote }}
  local_dir: {{ $s3ComputeLogManagerConfig.localDir | quote }}
  prefix: {{ $s3ComputeLogManagerConfig.prefix | quote }}
  use_ssl: {{ $s3ComputeLogManagerConfig.useSsl }}
  verify: {{ $s3ComputeLogManagerConfig.verify }}
  verify_cert_path: {{ $s3ComputeLogManagerConfig.verifyCertPath | quote }}
  endpoint_url: {{ $s3ComputeLogManagerConfig.endpointUrl | quote }}
{{- end }}