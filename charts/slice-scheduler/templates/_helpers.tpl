{{/*
Copyright 2026 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}}
{{/*
Expand the name of the chart.
*/}}
{{- define "slice-scheduler.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "slice-scheduler.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart labels
*/}}
{{- define "slice-scheduler.labels" -}}
helm.sh/chart: {{ include "slice-scheduler.name" . }}-{{ .Chart.Version }}
{{ include "slice-scheduler.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "slice-scheduler.selectorLabels" -}}
app.kubernetes.io/name: {{ include "slice-scheduler.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "slice-scheduler.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "slice-scheduler.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Render a 64-bit integer-seconds toleration value (defaulting unset/null to 300).
YAML numbers decode as float64 in Helm, so whole float64 values are formatted
without scientific notation while durations (e.g. "5m"), fractional decimals
(e.g. 600.9), booleans, empty values, and int64 overflows fail fast at render time.
*/}}
{{- define "slice-scheduler.tolerationSeconds" -}}
{{- $key := index . 0 -}}
{{- $val := index . 1 -}}
{{- if kindIs "invalid" $val -}}
300
{{- else -}}
{{- $rendered := "" -}}
{{- if kindIs "float64" $val -}}
{{- if eq $val (floor $val) -}}
{{- if eq $val 0.0 -}}
{{- $rendered = "0" -}}
{{- else -}}
{{- $rendered = printf "%.0f" $val -}}
{{- end -}}
{{- end -}}
{{- else if or (kindIs "int" $val) (kindIs "int64" $val) (kindIs "string" $val) -}}
{{- $rendered = printf "%v" $val -}}
{{- end -}}
{{- if or (not (regexMatch "^-?[0-9]+$" $rendered)) (ne (printf "%d" (int64 $rendered)) $rendered) -}}
{{- fail (printf "%s must be a 64-bit integer number of seconds (got %v)" $key $val) -}}
{{- end -}}
{{- $rendered -}}
{{- end -}}
{{- end }}
