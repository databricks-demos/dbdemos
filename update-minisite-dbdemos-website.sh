if ! aws sts get-caller-identity --profile aws-field-eng_databricks-power-user >/dev/null 2>&1; then
aws sso login --profile aws-field-eng_databricks-power-user
fi

aws --profile aws-field-eng_databricks-power-user s3 cp dbdemos/minisite s3://databricks-web-files/demos --recursive --acl bucket-owner-full-control
