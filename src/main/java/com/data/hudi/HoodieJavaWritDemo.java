package com.data.hudi;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.*;


/**
 * Simple examples of #{@link HoodieJavaWriteClient}.
 * <p>
 * Usage: HoodieJavaWriteClientExample <tablePath> <tableName>
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieJavaWriteClientExample file:///tmp/hoodie/sample-table hoodie_rt`
 */
public class HoodieJavaWritDemo {
    public static String PERSON_SCHEMA = "{\"type\":\"record\",\"name\":\"personrec\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"},{\"name\":\"job\",\"type\":\"string\"},{\"name\":\"salary\",\"type\":\"double\"}]}";
    public static Schema avroSchema = new Schema.Parser().parse(PERSON_SCHEMA);

    private static final Logger LOG = LogManager.getLogger(HoodieJavaWritDemo.class);

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

    public static void main(String[] args) throws Exception {

        String tablePath = "/Volumes/Samsung_T5/opensource/hudi_javaclient_learn/output/hudi/t_person";
        String tableName = "t_person";


        Configuration hadoopConf = new Configuration();
        // initialize the table, if not done already
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            // 使用自定义配置定义hudi 表配置
            final Properties properties = new Properties();
            // 指定非分区表
            properties.setProperty(KEY_GENERATOR_CLASS_NAME.key(), "org.apache.hudi.keygen.NonpartitionedKeyGenerator");
            properties.setProperty(PARTITION_FIELDS.key(), "");
            // 指定没有pre combine字段
            properties.setProperty(PRECOMBINE_FIELD.key(), "");
            // 初始化表
            HoodieTableMetaClient.withPropertyBuilder()
                    .fromProperties(properties)
                    .setTableType(tableType)
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
        }

        // 创建write client用于写入数据
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(PERSON_SCHEMA)
                .withParallelism(2, 2)
                .withDeleteParallelism(2).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

        HoodieJavaWriteClient<HoodieAvroPayload> client = new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

        // inserts
        String newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);

        final GenericData.Record record = new GenericData.Record(avroSchema);
        record.put("id",1L);
        record.put("name","pk");
        record.put("age",11);
        record.put("job","teacher");
        record.put("salary",333343.33);
        final HoodieAvroPayload hoodieAvroPayload = new HoodieAvroPayload(Option.of(record));
        final HoodieKey hoodieKey = new HoodieKey("1", "");
        final HoodieRecord<HoodieAvroPayload> hoodieRecord = new HoodieRecord<>(hoodieKey, hoodieAvroPayload);
        List<HoodieRecord<HoodieAvroPayload>> records = Lists.newArrayList(
                hoodieRecord
        );
        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);

        client.upsert(recordsSoFar, newCommitTime);

        //// updates
        //newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        //List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
        //records.addAll(toBeUpdated);
        //recordsSoFar.addAll(toBeUpdated);
        //writeRecords =
        //        recordsSoFar.stream().map(r -> new HoodieRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        //client.upsert(writeRecords, newCommitTime);
        //
        //// Delete
        //newCommitTime = client.startCommit();
        //LOG.info("Starting commit " + newCommitTime);
        //// just delete half of the records
        //int numToDelete = recordsSoFar.size() / 2;
        //List<HoodieKey> toBeDeleted =
        //        recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        //client.delete(toBeDeleted, newCommitTime);

        client.close();
    }
}
