# How to stream PostgreSQL changes to Amazon S3 with AWS Fargate

This repository contains the code and infrastructure template for the AWS Database Blog post "How to stream PostgreSQL changes to Amazon S3 with AWS Fargate."

## Architecture

```
RDS/Aurora PostgreSQL → Fargate CDC Reader → EventBridge → SQS → Lambda → S3
```

The pipeline captures row-level changes (INSERT, UPDATE, DELETE) from a PostgreSQL database using logical replication and writes them as JSON records to S3 in near real time.

## Repository structure

```
.
├── cdc-pipeline-cfn.yaml      # CloudFormation template (deploys the full pipeline)
├── cdc-reader/
│   ├── Dockerfile             # Container image for the CDC reader
│   ├── cdc_reader.py          # Python application that polls the replication slot
│   └── requirements.txt       # Python dependencies
└── README.md
```

## Prerequisites

- An Amazon RDS for PostgreSQL or Aurora PostgreSQL instance (PostgreSQL 14+)
- A VPC with at least two subnets in different Availability Zones
- The AWS CLI installed and configured
- Docker or [Finch](https://github.com/runfinch/finch) for building the container image
- Logical replication enabled on the source (`rds.logical_replication = 1`)

## Quick start

### 1. Create a dedicated replication user and configure the source database

Connect to your source database as the admin user and create a least-privilege role for CDC:

```sql
-- Create a dedicated user for CDC (do NOT use the master/superuser)
CREATE USER cdc_reader WITH LOGIN REPLICATION;
GRANT rds_replication TO cdc_reader;

-- Grant SELECT on the tables you want to replicate
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO cdc_reader;

-- Create the replication slot and publication
SELECT * FROM pg_create_logical_replication_slot('cdc_pipeline_slot', 'pgoutput');
CREATE PUBLICATION cdc_pipeline_pub FOR ALL TABLES;
```

We also recommend setting `rds.force_ssl=1` in your RDS parameter group to ensure all connections use TLS.

### 2. Store your database credentials in Secrets Manager

Create a JSON file with credentials (avoids leaking secrets to shell history):

```bash
cat > /tmp/cdc-creds.json << 'EOF'
{
  "username": "cdc_reader",
  "password": "<your-cdc-reader-password>",
  "host": "<your-endpoint>",
  "port": "5432",
  "dbname": "postgres"
}
EOF

aws secretsmanager create-secret \
  --name cdc-pipeline/db-credentials \
  --secret-string file:///tmp/cdc-creds.json

# Remove the temporary credentials file
rm /tmp/cdc-creds.json
```

### 3. Deploy the CloudFormation stack

```bash
aws cloudformation create-stack \
  --stack-name cdc-pipeline \
  --template-body file://cdc-pipeline-cfn.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameters \
    ParameterKey=VpcId,ParameterValue=<your-vpc-id> \
    ParameterKey=Subnet1,ParameterValue=<your-subnet-1> \
    ParameterKey=Subnet2,ParameterValue=<your-subnet-2> \
    ParameterKey=SourceDBEndpoint,ParameterValue=<your-db-endpoint> \
    ParameterKey=SourceDBSecurityGroup,ParameterValue=<your-db-sg-id> \
    ParameterKey=CreateVpcEndpoints,ParameterValue=true
```

Set `CreateVpcEndpoints` to `false` if your subnets have internet access and you don't need VPC endpoints.

### 4. Build and push the CDC reader container

```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(aws configure get region)

aws ecr get-login-password --region $REGION | \
  docker login --username AWS --password-stdin $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com

docker build --platform linux/amd64 \
  -t $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/cdc-reader:latest cdc-reader/

docker push $ACCOUNT_ID.dkr.ecr.$REGION.amazonaws.com/cdc-reader:latest
```

### 5. Start the CDC reader

```bash
aws ecs update-service \
  --cluster cdc-pipeline-cluster \
  --service cdc-reader-service \
  --desired-count 1
```

### 6. Test it

Insert a row into any table on your source database. Within about 10 seconds, a JSON file appears in the S3 bucket:

```bash
aws s3 ls s3://cdc-pipeline-output-$ACCOUNT_ID/ --recursive | tail -1
```

## Cleanup

```bash
# Stop the CDC reader
aws ecs update-service --cluster cdc-pipeline-cluster --service cdc-reader-service --desired-count 0

# Drop the replication slot (on the source database)
# SELECT pg_drop_replication_slot('cdc_pipeline_slot');

# Drop the publication (on the source database)
# DROP PUBLICATION cdc_pipeline_pub;

# Delete the stack
aws cloudformation delete-stack --stack-name cdc-pipeline

# Delete the secret
aws secretsmanager delete-secret --secret-id cdc-pipeline/db-credentials --force-delete-without-recovery
```

## Security

- All compute (Fargate, Lambda) can run in private subnets with no internet gateway
- VPC endpoints are created conditionally for private subnet deployments
- Database credentials are stored in AWS Secrets Manager
- The S3 bucket enforces encryption, versioning, and blocks public access
- IAM roles follow least-privilege principles

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
