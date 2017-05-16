### spark-in-space-app

Heroku Button deploy of a sample Spark Job repo that can be executed against a [spark-in-space](https://github.com/heroku/spark-in-space) cluster. 

requires: a private space with dns-discovery enabled, and an spark-in-space cluster running [spark-in-space](https://github.com/heroku/spark-in-space).

[![Deploy](https://www.herokucdn.com/deploy/button.svg)](https://heroku.com/deploy?template=https://github.com/heroku/spark-in-space-app)

Note: The rest of this readme assumes you have set your app name as `$app`, and your spark cluster app name as `$spark` in your shell, like so

```
app=my-spark-app
spark=my-spark-in-space-app
```


### managed and one-off spark jobs

The button deploy will build the job in this repo, and run it in a managed dyno, which will cause it to be re-submitted when it exits.

The simple job class is [here.](src/main/scala/spark/in/space/Job.scala)

once you see the job process come up, it should execute then exit and be restarted by the space control plane.

### S3 HDFS

You can use s3 as an hdfs compatible filesystem by installing the `bucketeer` addon, using the `--as SPARK_JOB_S3` option.

This is provided by the button deploy.

If you do this, bucketeer will set `SPARK_JOB_S3_BUCKET_NAME`, `SPARK_JOB_S3_AWS_ACCESS_KEY_ID`, `SPARK_JOB_S3_AWS_SECRET_ACCESS_KEY` config vars.

This will be detected and cause the writing of proper defaults to spark-defaults.conf. You can then use `s3a://` or `s3n://` urls in spark.

If you are deploying this app manually or dont need S3 HDFS, you can skip or remove the bucketeer adddon and the spark cluster should still function, just without S3 access.

If you want to bring your own S3 bucket, simply remove the bucketeer addon and manually set the configuration.


```
heroku addons:remove bucketeer -a $app
heroku config:set SPARK_JOB_S3_BUCKET_NAME=<the bucket you want to store notebooks in> -a $app
heroku config:set SPARK_JOB_S3_AWS_ACCESS_KEY_ID=<creds that can read and write your buckets> -a $app
heroku config:set SPARK_JOB_S3_AWS_SECRET_KEY=<creds that can read and write your buckets> -a $app
```

Any spark code you run from this app wil have the spark context configured with those credentials, so you should be able to access
`s3n://` and `s3a://` urls.


To try out the S3 functionality, you can do the following.

```
heroku run:inside console.1 bash -a your-spark-app
./bin/spark-shell

val bucket = sys.env("SPARK_JOB_S3_BUCKET_NAME")
val file = s"s3a://$bucket/test-object-file"
val ints = sc.makeRDD(1 to 10000)
ints.saveAsObjectFile(file)
### lots of spark output

:q

./bin/spark-shell
val bucket = sys.env("SPARK_JOB_S3_BUCKET_NAME")
val file = s"s3a://$bucket/test-object-file"
val theInts = sc.objectFile[Int](file)
theInts.reduce(_ + _)
### lots of spark output
res0: Int = 50005000
```

