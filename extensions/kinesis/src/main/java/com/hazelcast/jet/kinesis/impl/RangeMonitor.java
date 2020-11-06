package com.hazelcast.jet.kinesis.impl;

import com.amazonaws.services.kinesis.AmazonKinesisAsync;
import com.amazonaws.services.kinesis.model.ListShardsRequest;
import com.amazonaws.services.kinesis.model.ListShardsResult;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardFilter;
import com.hazelcast.logging.ILogger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class RangeMonitor extends AbstractShardWorker {

    /**
     * The maximum number of shards returned by a single ListShards call.
     */
    private static final int MAX_SHARD_BATCH_SIZE = 100;

    /**
     * We want to monitor only the currently open shards in the data stream.
     */
    private static final ShardFilter SHARD_FILTER = new ShardFilter().withType("AT_LATEST");

    private final int id;
    private final HashRange hashRange;
    private final Set<String> knownShards;
    private final RandomizedRateTracker listShardsRateTracker;
    private final ILogger logger;

    private State state = State.READY_TO_LIST_SHARDS;
    private String nextToken;
    private Future<ListShardsResult> listShardResult;
    private long nextListShardsTime;

    private List<Shard> newShards = new ArrayList<>();

    public RangeMonitor(
            int id,
            int totalInstances,
            AmazonKinesisAsync kinesis,
            String stream,
            HashRange hashRange,
            Collection<Shard> knownShards,
            ILogger logger
    ) {
        super(kinesis, stream);
        this.id = id;
        this.logger = logger;
        this.hashRange = hashRange;
        this.knownShards = knownShards.stream().map(Shard::getShardId).collect(toSet());
        this.listShardsRateTracker = new RandomizedRateTracker(1000 * totalInstances, 100); //todo: magic numbers
        this.nextListShardsTime = System.currentTimeMillis() + listShardsRateTracker.next();
    }

    public Result run() {
        try {
            switch (state) {
                case READY_TO_LIST_SHARDS:
                    if (System.currentTimeMillis() < nextListShardsTime) {
                        return Result.NOTHING;
                    }

                    ListShardsRequest request = buildListShardsRequest(stream, nextToken);
                    listShardResult = kinesis.listShardsAsync(request);
                    state = State.WAITING_FOR_SHARD_LIST;

                    nextListShardsTime = System.currentTimeMillis() + listShardsRateTracker.next();
                    //todo: This operation has a limit of 100 transactions per second per data stream. That means
                    // 100 / processors for this one
                    // also: RANDOMIZE!

                    return Result.NOTHING;


                case WAITING_FOR_SHARD_LIST:
                    if (listShardResult.isDone()) {
                        try {
                            ListShardsResult result = listShardResult.get();
                            nextToken = result.getNextToken();

                            List<Shard> shards = result.getShards();

                            List<Shard> unknownShards = shards.stream()
                                    .filter(shard -> KinesisUtil.shardBelongsToRange(shard, hashRange))
                                    .filter(shard -> !knownShards.contains(shard.getShardId())).collect(toList());

                            if (!unknownShards.isEmpty()) {
                                System.err.println(id + " - shardsInRange (" + hashRange + ")= " + shards.stream()
                                        .filter(shard -> KinesisUtil.shardBelongsToRange(shard, hashRange))
                                        .collect(toList())); //todo: remove
                                System.err.println("\tnextToken = " + nextToken); //todo: remove
                                System.err.println("\tknownShards = " + knownShards); //todo: remove
                                System.err.println("\tunknownShards = " + unknownShards); //todo: remove
                            }

                            if (unknownShards.isEmpty()) {
                                state = State.READY_TO_LIST_SHARDS;
                                return Result.NOTHING;
                            } else {
                                knownShards.addAll(unknownShards.stream().map(Shard::getShardId).collect(Collectors.toList()));
                                newShards.addAll(unknownShards);
                                state = State.NEW_SHARDS_FOUND;
                                return Result.NEW_SHARD;
                            }
                        } catch (ExecutionException e) {
                            logger.warning("Error encountered while reading active shard list. Will retry. Cause: " + e.getMessage());
                            //todo: exponential backoff?
                            //todo: handle some?
                            nextToken = null;
                            state = State.READY_TO_LIST_SHARDS;
                            return Result.NOTHING;
                        }
                    } else {
                        return Result.NOTHING;
                    }

                case NEW_SHARDS_FOUND:
                    newShards.remove(0);
                    state = newShards.isEmpty() ? State.READY_TO_LIST_SHARDS : State.NEW_SHARDS_FOUND;
                    return newShards.isEmpty() ? Result.NOTHING : Result.NEW_SHARD;

                default:
                    throw new RuntimeException("Programming error, unhandled state: " + state);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        }
    }

    public void removeShard(Shard shard) {
        /*boolean removed = knownShards.remove(shard.getShardId());
        if (!removed) {
            throw new IllegalArgumentException("Shard to be removed (" + shard.getShardId() + ") is unknown");
        }*/ //todo: how can we clean up the known set?
    }

    public Shard getNewShard() {
        if (newShards.isEmpty()) {
            throw new IllegalStateException("Can't ask for new shards observed when there are none");
        }
        return newShards.get(0);
    }

    enum Result {
        /**
         * Running the monitor has not produced any events that need handling.
         */
        NOTHING,

        /**
         * Running the monitor has lead to noticing a new shard that needs handling.
         */
        NEW_SHARD
    }

    private enum State {
        /**
         * Ready to request the up-to-date list of shards.
         */
        READY_TO_LIST_SHARDS,

        /**
         * Reading shard list initiated, waiting for the result.
         */
        WAITING_FOR_SHARD_LIST,

        /**
         * Monitor has discovered shards that aren't yet assigned readers.
         */
        NEW_SHARDS_FOUND,
    }

    private static ListShardsRequest buildListShardsRequest(String stream, String nextToken) {
        ListShardsRequest request = new ListShardsRequest();
        request.setMaxResults(MAX_SHARD_BATCH_SIZE);
        if (nextToken == null) {
            request.setStreamName(stream);
        } else {
            request.setNextToken(nextToken);
        }
        request.setShardFilter(SHARD_FILTER);
        return request;
    }

}
