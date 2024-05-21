Athena
======

AWS Athena is an interactive query service that makes it easy to analyze data in Amazon S3 using standard SQL. 
Athena is serverless, so there is no infrastructure to manage, and you pay only for the queries that you run.
Athena is easy to use. Simply point to your data in Amazon S3, define the schema, and start querying using standard SQL.
Most results are delivered within seconds. With Athena, there’s no need for complex ETL jobs to prepare your data for analysis.
This makes it easy for anyone with SQL skills to quickly analyze large-scale datasets.

Prerequisites
-------------

In order to use the athena module, you must have AWS account with following credentials:

.. code-block:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "CommoncrawlDB",
                "Effect": "Allow",
                "Action": [
                    "athena:CreateDataCatalog",
                    "glue:BatchCreatePartition",
                    "athena:StartQueryExecution",
                    "glue:CreateTable",
                    "glue:CreateDatabase",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:UpdateTable",
                    "glue:UpdatePartition",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "athena:GetQueryExecution",
                    "athena:ListTableMetadata",
                    "s3:GetBucketLocation",
                    "s3:DescribeJob"
                ],
                "Resource": "*"
            },
            {
                "Sid": "ResultsBucket",
                "Effect": "Allow",
                "Action": "s3:ListBucket",
                "Resource": "arn:aws:s3:::cmoncrawl-testbucket"
            },
            {
                "Sid": "ResultsBucketObjects",
                "Effect": "Allow",
                "Action": [
                    "s3:PutObject",
                    "s3:GetObject",
                    "s3:DeleteObject"
                ],
                "Resource": "arn:aws:s3:::cmoncrawl-testbucket/*"
            },
            {
                "Sid": "CommoncrawlBucket",
                "Effect": "Allow",
                "Action": [
                    "s3:GetObject",
                    "s3:ListBucket"
                ],
                "Resource": [
                    "arn:aws:s3:::commoncrawl/*",
                    "arn:aws:s3:::commoncrawl"
                ]
            }
        ]
    }

Caching
-------
If you provide a bucket name when itnializing the :py:class:`cmoncrawl.aggregator.athena_query.AthenaAggregator`,
the results of the query will be cached in the bucket. Whenever you make the same query the results will be reused.
This means that the bucket is not automatically cleaned up and it's your responsibility to do so.

If you don't provide a bucket name, the results will not be cached and randomly generated bucket will be used and deleted
after the query is finished.





