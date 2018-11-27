package de.hhu.bsinfo.dxram.boot;

import de.hhu.bsinfo.dxutils.CRC16;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class ZookeeperBootComponentTest {

    @Test
    @Ignore
    public void testDuplicateNodeIds() {

        Set<Short> set = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            assertTrue(String.format("Duplicate id detected after %d iterations", i), set.add(getNodeId(i)));
        }
    }

    private static short getNodeId(int p_counter) {
        int seed = 1;
        short nodeId = 0;

        for (int i = 0; i < p_counter; i++) {
            nodeId = CRC16.continuousHash(seed, nodeId);
            seed++;
        }

        return nodeId;
    }

}