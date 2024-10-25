# Clean up code formatting
```
poetry run pre-commit run --all-files
```

# Upload script
aws s3 cp main.py s3://abacus-internal-glue-artefacts/main.py

# Upload package
aws s3 cp dist/*.whl s3://abacus-internal-glue-artefacts

# Submit a new glue job

aws glue update-job --job-name abacus-job --job-update "{\"Command\": {\"Name\": \"glueetl\", \"PythonVersion\": \"3\", \"ScriptLocation\": \"s3://abacus-internal-glue-artefacts/main.py\"}, \"DefaultArguments\": {\"--extra-py-files\": \"s3://abacus-internal-glue-artefacts/src-0.1.0-py3-none-any.whl\"}}"
aws glue update-job --job-name abacus-job --job-update "{\"Role\": \"arn:aws:iam::925061584404:role/glue-service-account-role\", \"Command\": {\"Name\": \"glueetl\", \"ScriptLocation\": \"s3://abacus-internal-glue-artefacts/main.py\"}, \"DefaultArguments\": {\"--extra-py-files\": \"s3://abacus-internal-glue-artefacts/src-0.1.0-py3-none-any.whl\"}}"