## How to Add `target-iceberg` to Meltano
Run `meltano add --custom loader target-iceberg` 

Take the default value by pressing enter for name, namespace, settings and executable. However when it asks for pip_url:

Add: `git+https://github.com/taeefnajib/target-iceberg.git` 

In your `meltano.yml` file you'll see something like this:
```yml
...
loaders:
  - name: target-iceberg
    namespace: target_iceberg
    pip_url: git+https://github.com/taeefnajib/target-iceberg.git
    executable: target-iceberg
...
```
Add the config properties manually:

```yml
config:
      table_name: <<tablename>>
      aws_access_key: $AWS_ACCESS_KEY
      aws_secret_key: $AWS_SECRET_KEY
      aws_region: <<AWS bucket region>>
      hive_thrift_uri: <<thrift URI>>
      warehouse_uri: <<s3a URI for your bucket>>
      partition_by:
      - <<Column Name>>
```
Example:
```yml
config:
      table_name: testtable
      aws_access_key: $AWS_ACCESS_KEY
      aws_secret_key: $AWS_SECRET_KEY
      aws_region: us-east-1
      hive_thrift_uri: thrift://0.0.0.0:9083
      warehouse_uri: s3a://iceberg-data-lake/warehouse
      partition_by:
      - Month
```

Also, create a `.env` file and include the environment variables for AWS authentication:

```
export AWS_ACCESS_KEY="XXXXXXXXXXXXXXXXX"
export AWS_SECRET_KEY="XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"
```

Assuming you already have an extractor set up, you can start syncing. Example:
```bash
meltano el tap-csv target-iceberg
```