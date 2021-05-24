{{ define "service-dagit" }}

apiVersion: v1
kind: Service
metadata:
  name: {{ include "dagster.dagit.fullname" . }} {{- if .dagitReadOnly -}} -read-only {{- end }}
  labels:
    {{- include "dagster.labels" . | nindent 4 }}
    component: dagit {{- if .dagitReadOnly -}} -read-only {{- end }}
  annotations:
    {{- range $key, $value := .Values.dagit.service.annotations }}
    {{ $key }}: {{ $value | squote }}
    {{- end }}
spec:
  type: {{ .Values.dagit.service.type | default "ClusterIP" }}
  ports:
    - port: {{ .Values.dagit.service.port | default 80 }}
      protocol: TCP
      name: http
  selector:
    {{- include "dagster.selectorLabels" . | nindent 4 }}
    component: dagit {{- if .dagitReadOnly -}} -read-only {{- end }}

{{ end }}
