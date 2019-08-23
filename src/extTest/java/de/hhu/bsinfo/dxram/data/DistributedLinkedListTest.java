package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.dxmem.data.ChunkByteArray;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.DXRAMJunitRunner;
import de.hhu.bsinfo.dxram.DXRAMTestConfiguration;
import de.hhu.bsinfo.dxram.TestInstance;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.ChunkTestConstants;
import de.hhu.bsinfo.dxram.chunk.ChunkTestUtils;
import de.hhu.bsinfo.dxram.util.NetworkHelper;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.dxutils.NodeID;
import de.hhu.bsinfo.dxutils.data.holder.DistributableString;
import org.junit.Assert;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER),
        })
public class DistributedLinkedListTest {

    private static final long PARK_TIME = TimeUnit.SECONDS.toMillis(1);
    private static final String NAMESERVICE_ID = "MYLST";

    @TestInstance(runOnNodeIdx = 1)
    public void testDistributedWrite(final DXRAM dxram) {
        BootService bootService = dxram.getService(BootService.class);
        short peer = NetworkHelper.findPeer(bootService);
        assertNotEquals(NodeID.INVALID_ID, peer);

        // Create a linked list
        DistributedLinkedList<String> list = DistributedLinkedList.create(NAMESERVICE_ID, dxram, DistributableString::new);

        // Add (append) an element storing the data on a remote peer
        list.add("Hello", peer);

        // Add (append) an element storing the data on this peer
        list.add("World", bootService.getNodeID());

        // Add an element at a specific index storing the data on a remote peer
        list.add(1, "Distributed", peer);
    }

    @TestInstance(runOnNodeIdx = 2)
    public void testDistributedRead(final DXRAM dxram) {
        DistributedLinkedList<String> list = null;

        // Wait until first node created the linked list
        while (list == null) {
            try {
                list = DistributedLinkedList.get(NAMESERVICE_ID, dxram, DistributableString::new);
            } catch (ElementNotFoundException ignored) {}
        }

        // Wait until first node added two elements to the linked list
        while (list.size() != 3) {
            LockSupport.parkNanos(PARK_TIME);
        }

        // Join elements using one whitespace as the seperator
        String content = list.stream().collect(Collectors.joining(" "));

        assertEquals("Hello Distributed World", content);
    }
}
