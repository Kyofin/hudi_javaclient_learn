package com.data.hudi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Simple examples of #{@link HoodieJavaWriteClient}.
 * <p>
 * Usage: HoodieJavaWriteClientExample <tablePath> <tableName>
 * <tablePath> and <tableName> describe root path of hudi and table name
 * for example, `HoodieJavaWriteClientExample file:///tmp/hoodie/sample-table hoodie_rt`
 */
public class HoodieJavaWriteClientExample {

    private static final Logger LOG = LogManager.getLogger(HoodieJavaWriteClientExample.class);

    private static String tableType = HoodieTableType.COPY_ON_WRITE.name();

    public static void main(String[] args) throws Exception {

        String tablePath = "/Volumes/Samsung_T5/opensource/hudi_javaclient_learn/output/hudi/huditable";
        String tableName = "huditable";

        // Generator of some records to be loaded in.
        HoodieExampleDataGenerator<HoodieAvroPayload> dataGen = new HoodieExampleDataGenerator<>();

        Configuration hadoopConf = new Configuration();
        // initialize the table, if not done already
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableType)
                    .setTableType(HoodieTableType.COPY_ON_WRITE)
                    .setTableName(tableName)
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .initTable(hadoopConf, tablePath);
        }

        // Create the write client to write some records in
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
                .withSchema(HoodieExampleDataGenerator.TRIP_EXAMPLE_SCHEMA).withParallelism(2, 2)
                .withDeleteParallelism(2).forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
        HoodieJavaWriteClient<HoodieAvroPayload> client =
                new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

        // inserts
        String newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);

        List<HoodieRecord<HoodieAvroPayload>> records = dataGen.generateInserts(newCommitTime, 10);
        List<HoodieRecord<HoodieAvroPayload>> recordsSoFar = new ArrayList<>(records);
        List<HoodieRecord<HoodieAvroPayload>> writeRecords =
                recordsSoFar.stream().map(r -> new HoodieRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        client.upsert(writeRecords, newCommitTime);

        // updates
        newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);
        List<HoodieRecord<HoodieAvroPayload>> toBeUpdated = dataGen.generateUpdates(newCommitTime, 2);
        records.addAll(toBeUpdated);
        recordsSoFar.addAll(toBeUpdated);
        writeRecords =
                recordsSoFar.stream().map(r -> new HoodieRecord<HoodieAvroPayload>(r)).collect(Collectors.toList());
        client.upsert(writeRecords, newCommitTime);

        // Delete
        newCommitTime = client.startCommit();
        LOG.info("Starting commit " + newCommitTime);
        // just delete half of the records
        int numToDelete = recordsSoFar.size() / 2;
        List<HoodieKey> toBeDeleted =
                recordsSoFar.stream().map(HoodieRecord::getKey).limit(numToDelete).collect(Collectors.toList());
        client.delete(toBeDeleted, newCommitTime);

        client.close();
    }
}
