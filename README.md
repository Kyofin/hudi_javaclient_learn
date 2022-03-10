# Hudi Java客户端示例

## 写入hudi数据
参考`com.data.hudi.HoodieJavaWritDemo`
## sparksql查看hudi数据
```shell script
spark-shell  --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer'  --jars /Volumes/Samsung_T5/maven/repository/org/apache/hudi/hudi-spark-bundle_2.11/0.9.0/hudi-spark-bundle_2.11-0.9.0.jar
```
![](http://image-picgo.test.upcdn.net/img/20220310093405.png)
