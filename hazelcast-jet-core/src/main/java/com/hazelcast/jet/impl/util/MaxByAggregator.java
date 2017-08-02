package com.hazelcast.jet.impl.util;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.jet.impl.execution.init.JetImplDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.impl.Extractable;
import com.hazelcast.query.impl.getters.MultiResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * HZ-style aggregator to find and entry with maximum attribute value.
 * <p>
 * TODO delete after https://github.com/hazelcast/hazelcast/pull/11021 is merged.
 */
public class MaxByAggregator<I> extends Aggregator<I, I> implements IdentifiedDataSerializable {

    private String attributePath;
    private I maxEntry;
    private Comparable maxValue;

    public MaxByAggregator() {
        this(null);
    }

    public MaxByAggregator(String attributePath) {
        this.attributePath = attributePath;
    }

    @Override
    public final void accumulate(I entry) {
        Comparable extractedValue = extract(entry);
        if (extractedValue instanceof MultiResult) {
            boolean nullEmptyTargetSkipped = false;
            @SuppressWarnings("unchecked")
            MultiResult<Comparable> multiResult = (MultiResult<Comparable>) extractedValue;
            List<Comparable> results = multiResult.getResults();
            for (int i = 0; i < results.size(); i++) {
                Comparable result = results.get(i);
                if (result == null && multiResult.isNullEmptyTarget() && !nullEmptyTargetSkipped) {
                    // if a null or empty target is reached there will be a single null added to the multi-result.
                    // in aggregators we do not care about this null so we have to skip it.
                    // if there are more nulls in the multi-result, they have been added as values.
                    nullEmptyTargetSkipped = true;
                    continue;
                }
                accumulateExtracted(entry, results.get(i));
            }
        } else {
            accumulateExtracted(entry, extractedValue);
        }
    }

    /**
     * Extract the value of the given attributePath from the given entry.
     */
    @SuppressWarnings("unchecked")
    private <T> T extract(I input) {
        if (attributePath == null) {
            if (input instanceof Map.Entry) {
                return (T) ((Map.Entry) input).getValue();
            }
        } else if (input instanceof Extractable) {
            return (T) ((Extractable) input).getAttributeValue(attributePath);
        }
        throw new IllegalArgumentException("Can't extract " + attributePath + " from the given input");
    }

    private void accumulateExtracted(I entry, Comparable value) {
        if (isCurrentlyLessThan(value)) {
            maxValue = value;
            maxEntry = entry;
        }
    }

    private boolean isCurrentlyLessThan(Comparable otherValue) {
        if (otherValue == null) {
            return false;
        }
        return maxValue == null || maxValue.compareTo(otherValue) < 0;
    }

    @Override
    public void combine(Aggregator aggregator) {
        MaxByAggregator<I> maxAggregator = (MaxByAggregator<I>) aggregator;
        Comparable valueFromOtherAggregator = maxAggregator.maxValue;
        if (isCurrentlyLessThan(valueFromOtherAggregator)) {
            this.maxValue = valueFromOtherAggregator;
            this.maxEntry = maxAggregator.maxEntry;
        }
    }

    @Override
    public I aggregate() {
        return maxEntry;
    }

    @Override
    public int getFactoryId() {
        return JetImplDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getId() {
        return JetImplDataSerializerHook.MAX_BY_AGGREGATOR;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(attributePath);
        out.writeObject(maxValue);
        out.writeObject(maxEntry);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.attributePath = in.readUTF();
        this.maxValue = in.readObject();
        this.maxEntry = in.readObject();
    }
}
