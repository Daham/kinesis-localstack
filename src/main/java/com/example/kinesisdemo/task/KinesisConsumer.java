package com.example.kinesisdemo.task;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.model.*;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
@RequiredArgsConstructor
public class KinesisConsumer {

    @Autowired
    private AmazonKinesis amazonKinesis;

    private static final String KINESIS_STREAM_NAME = "TEST_KINESIS_STREAM";

    private static final int INTER_RECORD_CONSUME_TIME_GAP = 5000;

    private static final int RECORD_CONSUME_LIMIT = 100;

    private static final int RECORD_LIMIT = 25;

    @EventListener(ApplicationReadyEvent.class)
    public void configAndConsume() throws InterruptedException {

        //Iterate shards of the stream
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(KINESIS_STREAM_NAME);
        List<Shard> shards = new ArrayList<>();
        String exclusiveStartShardId = null;
        do {
            describeStreamRequest.setExclusiveStartShardId(exclusiveStartShardId);
            DescribeStreamResult describeStreamResult = amazonKinesis.describeStream(describeStreamRequest);
            shards.addAll(describeStreamResult.getStreamDescription().getShards());
            if (describeStreamResult.getStreamDescription().getHasMoreShards() && shards.size() > 0) {
                exclusiveStartShardId = shards.get(shards.size() - 1).getShardId();
            } else {
                exclusiveStartShardId = null;
            }
        } while (exclusiveStartShardId != null);

        //Get shard iterator for one shard (here the first one in the shard array)
        String shardIterator;
        GetShardIteratorRequest getShardIteratorRequest = new GetShardIteratorRequest();
        getShardIteratorRequest.setStreamName(KINESIS_STREAM_NAME);
        getShardIteratorRequest.setShardId(shards.get(0).getShardId());
        getShardIteratorRequest.setShardIteratorType("TRIM_HORIZON");

        GetShardIteratorResult getShardIteratorResult = amazonKinesis.getShardIterator(getShardIteratorRequest);
        shardIterator = getShardIteratorResult.getShardIterator();


        CompletableFuture.runAsync(() -> {
            try {
                getRecordsFromShard(shardIterator);
            } catch (InterruptedException e) {
                log.error("Error while getting records from the shard");
            }
        });
    }

    private void getRecordsFromShard(String shardIterator) throws InterruptedException {

        int counter = 0;
        //Consume the chosen shard using the shard iterator
        while (counter < RECORD_CONSUME_LIMIT) {
            GetRecordsRequest getRecordsRequest = new GetRecordsRequest();
            getRecordsRequest.setShardIterator(shardIterator);
            getRecordsRequest.setLimit(RECORD_LIMIT);

            GetRecordsResult getRecordsResult = amazonKinesis.getRecords(getRecordsRequest);
            List<Record> records = getRecordsResult.getRecords();

            records.forEach(record -> {
                try {
                    log.info("Consumed message {}", new String(record.getData().array(), StandardCharsets.UTF_8.toString()));
                } catch (UnsupportedEncodingException e) {
                    log.error("Error while stringify the data byte buffer array");
                }
            });

            Thread.sleep(INTER_RECORD_CONSUME_TIME_GAP);
            counter++;
        }




    }
}
