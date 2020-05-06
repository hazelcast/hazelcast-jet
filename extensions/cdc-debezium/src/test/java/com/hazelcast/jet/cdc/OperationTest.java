package com.hazelcast.jet.cdc;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OperationTest {

    @Test
    public void getValid() throws ParsingException {
        assertEquals(Operation.UNSPECIFIED, Operation.get(null));

        assertEquals(Operation.SYNC, Operation.get("r"));
        assertEquals(Operation.INSERT, Operation.get("c"));
        assertEquals(Operation.UPDATE, Operation.get("u"));
        assertEquals(Operation.DELETE, Operation.get("d"));
    }

    @Test(expected = ParsingException.class)
    public void getInvalid_SingleChar() throws ParsingException {
        assertEquals(Operation.DELETE, Operation.get("z"));
    }

    @Test(expected = ParsingException.class)
    public void getInvalid_MultiChar() throws ParsingException {
        assertEquals(Operation.DELETE, Operation.get("cx"));
    }

}
