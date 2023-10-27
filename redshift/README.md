# cdc to redshift

## 介绍
该样例提供了一个通过Glue实现从kafka摄入CDC数据，实时同步至Redshift的方法。


#### Create Glue Job
以下脚本用与通过命令行的方式在Glue创建一个ETL Job。

```shell
export JOB_NAME=cdc-to-redshift
export GLUE_EXECUTION_ROLE=<glue-execution-role>
export AWS_REGION=<region>
export JOB_CONFIG_FILE=<job-config-file-path>
export JOB_SCRIPT_PATH=<job-script-path>
export REDSHIFT_CONNECT=<redshift-glue-connection>
export MSK_CONNECT=<msk-glue-connection>
export GLUE_DATABASE_KAFKA=<glue-catalog-database>
export GLUE_TABLE_KAFKA=<glue-catalog-table>
export REDSHIFT_TMPDIR=<s3-path-for-redshift-temp>
export REDSHIFT_IAM_ROLE_ARN=<redshift-iam-role-arn>

aws glue create-job \
    --name $JOB_NAME \
    --role $GLUE_EXECUTION_ROLE \
    --command '{ 
        "Name": "gluestreaming", 
		"PythonVersion": "3", 
        "ScriptLocation": "'$JOB_SCRIPT_PATH'" 
    }' \
    --region $AWS_REGION \
    --connections '{"Connections":["'$REDSHIFT_CONNECT'","'$MSK_CONNECT'"]}' \
    --output json \
    --default-arguments '{ 
        "--job-language": "python", 
		"--additional-python-modules":"redshift_connector,jproperties", 
        "--aws_region": "'$AWS_REGION'", 
        "--conf": "--conf spark.executor.cores=8 --conf spark.sql.shuffle.partitions=1  --conf spark.default.parallelism=1 --conf spark.speculation=false", 
        "--config_s3_path": "'$JOB_CONFIG_FILE'", 
        "--enable-spark-ui": "false",
        "--redshift_connect": "'$REDSHIFT_CONNECT'",
        "--region": "'$AWS_REGION'",
        "--redshift_tmpdir": "'$REDSHIFT_TMPDIR'",
        "--redshift_iam_role": "'$REDSHIFT_IAM_ROLE_ARN'",
        "--glue_database_kafka": "'$GLUE_DATABASE_KAFKA'",
        "--glue_table_kafka": "'$GLUE_TABLE_KAFKA'",
        "--starting_offsets_of_kafka_topic": "latest"
    }' \
    --glue-version 4.0 \
    --number-of-workers 2 \
    --worker-type G.1X

aws glue start-job-run --job-name $JOB_NAME
```