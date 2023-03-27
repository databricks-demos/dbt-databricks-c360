dbt run-operation databricks_copy_into --args "
target_table: [target_table]
source: 's3://[your_bucket]/path'
file_format: csv
source_credential:
  AWS_ACCESS_KEY: '[temp_access_key_id]'
  AWS_SECRET_KEY: '[temp_secret_access_key]'
  AWS_SESSION_TOKEN: '[temp_session_token]
format_options:
  mergeSchema: 'true'
  header: 'true'
copy_options:
  mergeSchema: 'true'
"