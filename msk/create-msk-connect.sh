#!/bin/bash

#parameter
CLOUDFORMATION_STACK_NAME=glue-workshop
S3_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $CLOUDFORMATION_STACK_NAME | jq --raw-output '.Stacks[0].Outputs[]|select(.OutputKey=="S3Bucket").OutputValue')

MSK_CONNECT_NAME=glue-workshop-mskconnect
MYSQL_USERNAME=$(aws secretsmanager get-secret-value --secret-id MYSQLDBSecret | jq --raw-output '.SecretString' | jq -r .username)
MYSQL_PASSWORD=$(aws secretsmanager get-secret-value --secret-id MYSQLDBSecret | jq --raw-output '.SecretString' | jq -r .password)
MYSQL_HOSTNAME=$(aws rds describe-db-instances --db-instance-identifier mydbinstance | jq --raw-output '.DBInstances[0].Endpoint.Address')

MSK_SERVERLESS_CLUSTER_NAME=msk-serverless-glue-workshop
MSK_SERVERLESS_ARN=$(aws kafka list-clusters-v2 --cluster-name-filter $MSK_SERVERLESS_CLUSTER_NAME | jq --raw-output '.ClusterInfoList[0].ClusterArn')
MSK_SERVERLESS_BOOTSTRAP=$(aws kafka get-bootstrap-brokers --cluster-arn $MSK_SERVERLESS_ARN | jq --raw-output '.BootstrapBrokerStringSaslIam')

MYSQL_DATABASE=covid19db

MSK_VPC_SUBNET=$(aws kafka describe-cluster-v2 --cluster-arn $MSK_SERVERLESS_ARN | jq --raw-output '.ClusterInfo.Serverless.VpcConfigs[0].SubnetIds')
MSK_VPC_SG=$(aws kafka describe-cluster-v2 --cluster-arn $MSK_SERVERLESS_ARN | jq --raw-output '.ClusterInfo.Serverless.VpcConfigs[0].SecurityGroupIds')

# MSK_VPC=$(aws kafka describe-cluster-v2 --cluster-arn $MSK_SERVERLESS_ARN | jq --raw-output '.ClusterInfo.Serverless.VpcConfigs[0]')

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
SERVICE_EXECUTION_ROLEARN=arn:aws:iam::$ACCOUNT_ID:role/GlueWorkShopRoleForKafkaConnect

## Create MSK Connect Plugin
CUSTOM_PLUGIN_ARN=$(aws kafkaconnect create-custom-plugin --content-type ZIP --name mysql-cdc-plugin \
    --location "{\"s3Location\": {\"bucketArn\":\"arn:aws:s3:::$S3_BUCKET_NAME\",\"fileKey\": \"debezium-debezium-connector-mysql-1.9.7.zip\"}}" \
    | jq --raw-output '.customPluginArn')

echo "Create custom plugin success."

# Create MSK Connect Configuration
base64 > connect-configuration.properties <<EOF
key.converter=org.apache.kafka.connect.storage.StringConverter
key.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=false
config.storage.replication.factor=3
offset.storage.replication.factor=3
status.storage.replication.factor=3
offset.storage.partitions=3
status.storage.partitions=3
config.storage.partitions=3
offset.storage.topic=msk_connect_offsets_mysql-cdc-conect
cleanup.policy=compact
EOF

BASE64_CONTENT=$(awk BEGIN{RS=EOF}'{gsub(/\n/,"");print}' connect-configuration.properties)

RANDOM_STR=$(echo $RANDOM | base64 |cut -c 1-8)
WORK_CONFIGURATION_ARN=$(aws kafkaconnect create-worker-configuration --name mysql-cdc-connect-config-$RANDOM_STR --properties-file-content $BASE64_CONTENT | jq --raw-output '.workerConfigurationArn')

echo "Craete work configuration success."

#create msk connect json file
cat > create-msk-connect-$MSK_CONNECT_NAME.json <<EOF
{
  "connectorConfiguration": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "$MYSQL_HOSTNAME",
    "database.port": "3306",
    "database.user": "$MYSQL_USERNAME",
    "database.password": "$MYSQL_PASSWORD",
    "database.server.id": "123456",
    "database.server.name": "norrisdb",
    "database.include.list": "$MYSQL_DATABASE",
    "database.connectionTimeZone": "UTC",
    "database.history.kafka.topic": "dbhistory.norrisdb",
    "database.history.kafka.bootstrap.servers": "$MSK_SERVERLESS_BOOTSTRAP",
    "database.history.consumer.security.protocol": "SASL_SSL",
    "database.history.consumer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.consumer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.consumer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "database.history.producer.security.protocol": "SASL_SSL",
    "database.history.producer.sasl.mechanism": "AWS_MSK_IAM",
    "database.history.producer.sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "database.history.producer.sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
    "include.schema.changes": "true",
    "topic.creation.default.partitions": "3",
    "topic.creation.default.replication.factor": "3",
    "topic.creation.enable": "true",
    "output.data.format": "JSON",
    "tombstones.on.delete": "false",
    "decimal.handling.mode": "string",
    "insert.mode": "upsert",
    "time.precision.mode": "connect",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false"
  },
  "connectorName": "$MSK_CONNECT_NAME",
  "kafkaCluster": {
    "apacheKafkaCluster": {
      "bootstrapServers": "$MSK_SERVERLESS_BOOTSTRAP",
      "vpc": {
        "subnets": $MSK_VPC_SUBNET,
        "securityGroups": $MSK_VPC_SG
      }
    }
  },
  "capacity": {
    "provisionedCapacity": {
      "mcuCount": 2,
      "workerCount": 1
    }
  },
  "kafkaConnectVersion": "2.7.1",
  "serviceExecutionRoleArn": "$SERVICE_EXECUTION_ROLEARN",
  "plugins": [{
    "customPlugin": {
      "customPluginArn": "$CUSTOM_PLUGIN_ARN",
      "revision": 1
    }
  }],
  "kafkaClusterEncryptionInTransit": {
    "encryptionType": "TLS"
  },
  "kafkaClusterClientAuthentication": {
    "authenticationType": "IAM"
  },
  "workerConfiguration": {
    "workerConfigurationArn": "$WORK_CONFIGURATION_ARN",
    "revision": 1
  }
}
EOF

# Create log group
aws logs create-log-group --log-group-name /msk/msk-connect-logs/
echo "Craete cloudwatch log group."
# Create kafka connect
aws kafkaconnect create-connector --cli-input-json file://create-msk-connect-$MSK_CONNECT_NAME.json --log-delivery '
{
  "workerLogDelivery": {
      "cloudWatchLogs": {
        "enabled": true,
        "logGroup": "/msk/msk-connect-logs/"
      }
    }
}
'

echo "Craete kafka connect success."

