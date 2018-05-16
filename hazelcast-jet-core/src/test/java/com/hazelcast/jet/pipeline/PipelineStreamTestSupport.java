package com.hazelcast.jet.pipeline;

import com.hazelcast.core.HazelcastInstance;
import org.junit.Before;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;

public abstract class PipelineStreamTestSupport extends PipelineTestSupport {

    private final String journaledSrcMapName = journaledMapName();

    StreamStage<Integer> mapJournalSrcStage;
    List<String> inputKeys;

    // Windowing tests use input items as timestamps. This list contains items
    // that will advance the watermark on all partitions enough to close all
    // open windows.
    List<Integer> closingItems;

    @Before
    public void beforePipelineStreamTestSupport() {
        HazelcastInstance hz = member.getHazelcastInstance();
        int partitionCount = getPartitionService(hz).getPartitionCount();
        itemCount = 2 * partitionCount;
        inputKeys = IntStream.range(0, partitionCount)
                             .mapToObj(i -> generateKeyForPartition(hz, i))
                             .collect(toList());
        closingItems = nCopies(inputKeys.size(), 16 * itemCount);
        srcMap = jet().getMap(journaledSrcMapName);
        mapJournalSrcStage = drawEventJournalValues(journaledSrcMapName);
    }

    StreamStage<Integer> drawEventJournalValues(String mapName) {
        return p.drawFrom(Sources.mapJournal(mapName, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));
    }

    void addToMapJournal(Map<String, Integer> map, List<Integer> items) {
        Iterator<String> keyIter = inputKeys.iterator();
        for (Integer item : items) {
            if (!keyIter.hasNext()) {
                keyIter = inputKeys.iterator();
            }
            map.put(keyIter.next(), item);
        }
    }

    void addToSrcMapJournal(List<Integer> items) {
        addToMapJournal(srcMap, items);
    }
}
