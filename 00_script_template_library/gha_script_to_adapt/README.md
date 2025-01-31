# Pipeline documentation

## Table of Contents

- [Common setup](#common-setup)
- [Zoho Desk Sync](#zoho-desk-sync)
- [Data download and processing](#data-download-and-processing)
- [Data testing](#data-testing)
- [Data upload](#data-upload)
- [Best practices](#best-practices)

## Common setup

### Execution environment
- Self-hosted GitHub "runners" are used to execute the defined jobs
- Runners are residing at Bytesource production account's EKS
- Runners tagged as "cat_tenants" will be used to execute workflow jobs

### Environment variables
- `(DEV|PROD)_ROLE_ARN`: defines an AWS role the pipeline will assume, depending on the configured environment
- `SECRET_SUFFIX` or `SECRET_NAME`: the pipeline depends on AWS Secrets Manager stored secrets. Secrets will be available as environment variables for the pipeline executed code to consume. The AWS tenant (for eg cat-be, cat-ks, etc) needs to be configured in the appropriate Terraform configuration ([for eg](https://github.com/bytesource-team-klout/cat-tenants-configs/blob/main/dev/cat_tenant_be.tf#L19)) that a tenant specific secret store is created. Since the secret store is created in the format of `<TENANT_NAME>-scraper-secrets` (for eg: `cat-be-scraper-secrets`), this environment variable defines the suffix that will be appended to the `TENANT_NAME`
- `BUCKET_PREFIX`: There is 1-1 S3 bucket created for both DEV and PROD environments that the scraper jobs will use. S3 bucket names are globally unique at AWS - as such: based on the environment, this prefix will be used to determine the full name of the S3 bucket that the application will be provided via an environment variable `S3_BUCKET_NAME` or `S3_BUCKET` - see details below

## Zoho Desk sync

### Source
[zohosync.yml](https://github.com/bytesource-team-klout/cat-scraping/blob/main/.github/workflows/zohosync.yml)

### Trigger
- Pipeline is executed at a pre-defined schedule: every day at 3 am UTC
- Manually/on-demand

### Job defined
This section provides explanation for jobs and steps.
- There is 1 job defined called `sync`
    - Maximum runtime is set to 60 minutes
    - Input parameters for `jobs/sync/strategy/matrix/project_name` needs to be defined in the format of `"environment:project_directory_name"` for eg: `"DEV:CAT-BE-DEV"`, where
        - `environment` should be either `DEV` or `PROD`
        - `project_directory_name` should be a valid (case sensitive) directory name under the `Project` directory of this repository
            - name of the Project directory should match the tenant's name
            - project directory name might contain "-DEV" suffix (ie CAT-BE-DEV) in case the same project name appears without the "-DEV" suffix as well: for eg: CAT-BE Project directory also exists and represents the PROD infra related Project code
        - the `sync` job's steps will be executed for each of the configured `project_name` matrix
        - jobs are executed parallel
            - 3 parallel execution is permitted
        - failure of 1 job will NOT automatically fail execution of another job (jobs are independently executed)
- Set of steps:
    - repository checkout: see details of re-executing of failed task at [best practices](#best-practices)
    - determining runtime environment: the environment will be either "PROD" or "DEV". Setup `ROLE_ARN` variable that will be used as the AWS least privilege account that will be assumed by the pipeline
    - variable conversions: step is used to setup values for variables such as:
        - `PROJECT_DIR`: derived from the matrix/project_name column separated list - this is the directory name under the Project subdirectory of the repository
        - `SECRET_NAME`: derieved from the `PROJECT_DIR`: converted lower-case and appended with the `SECRET_SUFFIX` env var
            - in case name contains "-dev" suffix, scrub the last of such suffixes since this suffix is not actually used at AWS upon DEV env tenant setup
        - `TENANT_NAME`: derieved from the `PROJECT_DIR`: converted lower-case
            - if the name contains "-dev" suffix then scrub the last of such suffixes since this suffix is not actually used at AWS upon DEV env tenant setup
    - assume role at AWS: role session name will provide better visibility and tracing at AWS about access
    - setup of Python environment including "pipenv" and "awscli" packages which are requited later on for the Python script execution
    - find S3 bucket name: we're finding here the S3 bucket's name for the environment the pipeline is run on with the help of the `BUCKET_PREFIX` variable - S3 bucket is globally unique: DEV and PROD env will have different name but a matching prefix
    - execution of the `run_update.sh` script in the `Project/<PROJECT_DIR>/zoho_desk_sync/update_cycle_GHA` directory:
        - since the steps are including RDS database updates, we're using the `tools/github_actions/create_tunnel.sh` script to setup a Session Manager tunnel via the `bastion-host-ng` EC2 host
            - `session-manager-plugin.deb` dependency is installed for the `create_tunnel.sh` script
            - maximum 4 attempts are made to establish tunnel connection, 5 seconds apart: if setup of tunnel fails, pipeline will fail
        - the file called `.env_gha` is renamed to `.env`
            - see details at [Best practices](#best-practices)
        - pipenv virtual environment is setup
            - necessary Python modules are installed using the `Pipfile` contents and `Pipfile.lock` dependency tree
        - executing the `run_update.sh` script within the pipenv Python venv
        - the `TENANT_NAME` and the `S3_BUCKET_NAME` environment variables and their values are provided for the runtimes
    - finally: if there was an exeuction failure in the preceeding steps (exit code of non-zero), then an informational Slack notification will be sent to the [#cat-scraping-pipeline-alerts](https://bytesource.slack.com/archives/C072WD9236D) channel so members of the channel will quickly learn of a failure with possible follow-up actions

## Data download and processing
Scraper jobs are using a pre-defined set of steps in reference to the [template](https://github.com/bytesource-team-klout/cat-scraping/tree/main/Project/new_project_template). This specific workflow will execute steps 1a, 2a and 3a.
Where:
- scripts of 1a will gather/publish data into 1b directory
- 1b is input of scripts in 2a which would produce output data into 2b
- 2b is input of scripts in 3a which would produce output data into 3b

1b, 2b, and 3b are uploaded into the S3_BUCKET in the format of: `<S3_BUCKET>/<tenant_name>/datasets/<yyyymmdd_hhmmss_utc>/[123]b*/` where `yyyymmdd_hhmmss_utc` is the specific dataset's name.

### Source
[data-processing.yml](https://github.com/bytesource-team-klout/cat-scraping/blob/main/.github/workflows/data-processing.yml)

### Trigger
- Manually/on-demand

### Inputs
- Project name: provide a sub-directory name from the `Project` directory. Case sensitive - currently all subdirectories are upper case; will be used to determine the tenant name
- Environment: `DEV` or `PROD` - it will be used to determine whether pipeline jobs will be executed on Development or Production AWS account

### Jobs defined
Jobs will follow one another: `data-fetch-1a` is pre-requisite of `data-processing-2a` which is pre-requisite of `data-enrichment-3a`

## Data testing
Scraper jobs are using a pre-defined set of steps in reference to the [template](https://github.com/bytesource-team-klout/cat-scraping/tree/main/Project/new_project_template). This specific workflow will execute steps 5a: dataset downloaded from `<S3_BUCKET>/<tenant_name>/datasets/<yyyymmdd_hhmmss_utc>`. Workflow will publish test results at `<S3_BUCKET>/<tenant_name>/test_results/<yyyymmdd_hhmmss_utc>/<YYYYMMDD_HHMMSS_utc>`, where:
- the 1st `yyyymmdd_hhmmss_utc` subdirectory is the dataset name
- the 2nd `YYYYMMDD_HHMMSS_utc` subdirectory is the test execution time
    - multiple tests can be executed on the same dataset

### Source
[data-testing.yml](https://github.com/bytesource-team-klout/cat-scraping/blob/main/.github/workflows/data-testing.yml)

### Trigger
- Manually/on-demand

### Inputs
- Project name: provide a sub-directory name from the `Project` directory. Case sensitive - currently all subdirectories are upper case; will be used to determine the tenant name
- Environment: `DEV` or `PROD` - it will be used to determine whether pipeline jobs will be executed on Development or Production AWS account
- Dataset at S3 bucket: either `LATEST` or a specific (any past) dataset name residing in the S3 bucket for the given tenant for eg `20240328_084742_utc`. If `LATEST` (which is default) provided, then pipeline will determine the most recently created dataset name
- Sample size for data testing: test scripts will be using this many data samples during testing - or provide `all` to test through all data 

## Data upload
Scraper jobs are using a pre-defined set of steps in reference to the [template](https://github.com/bytesource-team-klout/cat-scraping/tree/main/Project/new_project_template). This specific workflow will execute step 4.
In case the upload script name contains the `postgres` string, then a Session Manager tunnel is established via the `bastion-host-ng` EC2 instance for providing access to the RDS database cluster via the designated TCP port.

### Source
[data-upload.yml](https://github.com/bytesource-team-klout/cat-scraping/blob/main/.github/workflows/data-upload.yml)

### Trigger
- Manually/on-demand

### Inputs
- Project name: provide a sub-directory name from the `Project` directory. Case sensitive - currently all subdirectories are upper case; will be used to determine the tenant name
- Environment: `DEV` or `PROD` - it will be used to determine whether pipeline jobs will be executed on Development or Production AWS account
- Dataset at S3 bucket: either `LATEST` or a specific (any past) dataset name residing in the S3 bucket for the given tenant for eg `20240328_084742_utc`. If `LATEST` (which is default) provided, then pipeline will determine the most recently created dataset name
- Delete data before upload new data: scripts will delete existing data from DB before uploading new data

## Best practices
For application code using the pipeline the below best practices can be established:
- in case of exception tracking, preferred to use a non-zero exit code so pipeline execution will be stop/fail triggered and notification is sent to the proper channel for follow-up
- both the pipeline and the pipenv virtual environment will load the `.env` file contents when execution occurs within the application's working directory
    - the application scripts are using contents of the `.env` file to provide configuration parameters and associate values such as `POSTGRESQL_DB_HOST`, `POSTGRESQL_DB_NAME`, `DEPARTMENT_ID` etc.
    - as contents of the `.env` file could have different values during "local development" effort compare to the GitHub runner environment, and those local `.env` values might be committed into the repository by mistake (breaking the pipeline), it's best to separate "local development" and GitHub runner configuration values: because of this a `.env_gha` file in the application directory should contain the environment variables that are required to be present at the GitHub runner
    - `.env_gha` will be renamed by the pipeline to `.env` upon execution of the jobs
- avoid placing secrets (such as tokens, passwords or other sensitive data) into `.env` or `.env_gha` or the application code or any other configuration file that gets committed into the repository. Instead: utilize the tenant's AWS Secrets Management secret (for eg <TENANT_NAME>-scraper-secret) - see also details for `SECRET_SUFFIX` [here](#environment-variables)
- in case of referencing date/time values (for eg placing date/time into file names), consider that runners are executing application with UTC timezone (0 GMT offset, disregarding any DST offset). It is best to produce code that is already using UTC timezone to avoid conflict between local PC and runner execution.
- tenant related data expected to be stored on the shared S3 bucket (bucket name is different on DEV and PROD environments), where the `S3_BUCKET_NAME` and `TENANT_NAME` are configuration values are provided by the pipeline in environment variables
    - reference format: `s3://<S3_BUCKET_NAME>/<TENANT_NAME>/zohodesk-data/*`
- re-executing failed tasks: upon the checkout step of the pipeline, the checkout of the repository is done through SHA value of the latest commit that was done on the selected branch (ideally main). In case there is a pipeline execution failure due to issues with the code and so a new commit is made into the selected branch, re-executing the failed task, will keep on using the same (now old) commit ID. Meaning re-running the failed job will not include changes made with a new commit.
