package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxmem.core.Address;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.*;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.util.NodeRole;

import org.junit.Assert;
import org.junit.runner.RunWith;

@RunWith(DXRAMJunitRunner.class)
@DXRAMTestConfiguration(
        nodes = {
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.SUPERPEER),
                @DXRAMTestConfiguration.Node(nodeRole = NodeRole.PEER)
        })
public class DirectSimpleChunkTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);
        BootService bootService = p_instance.getService(BootService.class);

        DirectAccessSecurityManager.init(
                bootService,
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testID(final DXRAM p_instance) {
        final long chunk = DirectSimpleChunk.create();
        final long address = DirectSimpleChunk.getAddress(chunk);
        int id = 10;

        DirectSimpleChunk.setVid(chunk, id);
        Assert.assertEquals(id, DirectSimpleChunk.getVid(chunk));
        Assert.assertEquals(id, DirectSimpleChunk.getVid(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(id, simpleChunk.getVid());
        }

        id++;
        DirectSimpleChunk.setVid(address, id);
        Assert.assertEquals(id, DirectSimpleChunk.getVid(chunk));
        Assert.assertEquals(id, DirectSimpleChunk.getVid(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(id, simpleChunk.getVid());
        }

        id++;
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            simpleChunk.setVid(id);
        }
        Assert.assertEquals(id, DirectSimpleChunk.getVid(chunk));
        Assert.assertEquals(id, DirectSimpleChunk.getVid(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(id, simpleChunk.getVid());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testName(final DXRAM p_instance) {
        final long chunk = DirectSimpleChunk.create();
        final long address = DirectSimpleChunk.getAddress(chunk);
        final String name = "testName";

        Assert.assertNull(DirectSimpleChunk.getName(chunk));
        Assert.assertNull(DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertNull(simpleChunk.getName());
        }

        DirectSimpleChunk.setName(chunk, name);
        Assert.assertEquals(name, DirectSimpleChunk.getName(chunk));
        Assert.assertEquals(name, DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            Assert.assertEquals(name, simpleChunk.getName());
        }

        DirectSimpleChunk.setName(chunk, null);
        Assert.assertNull(DirectSimpleChunk.getName(chunk));
        Assert.assertNull(DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertNull(simpleChunk.getName());
        }

        DirectSimpleChunk.setName(address, name);
        Assert.assertEquals(name, DirectSimpleChunk.getName(chunk));
        Assert.assertEquals(name, DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            Assert.assertEquals(name, simpleChunk.getName());
        }

        DirectSimpleChunk.setName(address, "");
        Assert.assertEquals("", DirectSimpleChunk.getName(chunk));
        Assert.assertEquals("", DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            Assert.assertEquals("", simpleChunk.getName());
        }

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            simpleChunk.setName(name);
        }
        Assert.assertEquals(name, DirectSimpleChunk.getName(chunk));
        Assert.assertEquals(name, DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            Assert.assertEquals(name, simpleChunk.getName());
        }

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            simpleChunk.setName(null);
        }
        Assert.assertNull(DirectSimpleChunk.getName(chunk));
        Assert.assertNull(DirectSimpleChunk.getName(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertNull(simpleChunk.getName());
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testNumbers(final DXRAM p_instance) {
        final long chunk = DirectSimpleChunk.create();
        final long address = DirectSimpleChunk.getAddress(chunk);
        final int[] init = new int[] { -1, -1, -1 };
        final int[] numbers = new int[] { 1, 2, 3 };

        Assert.assertEquals(0, DirectSimpleChunk.getNumbersLength(chunk));
        Assert.assertEquals(0, DirectSimpleChunk.getNumbersLength(address));
        Assert.assertNull(DirectSimpleChunk.getNumbers(chunk));
        Assert.assertNull(DirectSimpleChunk.getNumbers(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(0, simpleChunk.getNumbersLength());
            Assert.assertNull(simpleChunk.getNumbers());
        }

        DirectSimpleChunk.setNumbers(chunk, init);
        Assert.assertEquals(init.length, DirectSimpleChunk.getNumbersLength(chunk));
        Assert.assertEquals(init.length, DirectSimpleChunk.getNumbersLength(address));
        Assert.assertArrayEquals(init, DirectSimpleChunk.getNumbers(chunk));
        Assert.assertArrayEquals(init, DirectSimpleChunk.getNumbers(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(init.length, simpleChunk.getNumbersLength());
            Assert.assertArrayEquals(init, simpleChunk.getNumbers());
        }
        for (int i = 0; i < init.length; i++) {
            Assert.assertEquals(init[i], DirectSimpleChunk.getNumbers(chunk, i));
            Assert.assertEquals(init[i], DirectSimpleChunk.getNumbers(address, i));
            try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
                Assert.assertEquals(init[i], simpleChunk.getNumbers(i));
            }
        }

        DirectSimpleChunk.setNumbers(chunk, 0, numbers[0]);
        DirectSimpleChunk.setNumbers(address, 1, numbers[1]);
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            simpleChunk.setNumbers(2, numbers[2]);
        }
        Assert.assertEquals(numbers.length, DirectSimpleChunk.getNumbersLength(chunk));
        Assert.assertEquals(numbers.length, DirectSimpleChunk.getNumbersLength(address));
        Assert.assertArrayEquals(numbers, DirectSimpleChunk.getNumbers(chunk));
        Assert.assertArrayEquals(numbers, DirectSimpleChunk.getNumbers(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(numbers.length, simpleChunk.getNumbersLength());
            Assert.assertArrayEquals(numbers, simpleChunk.getNumbers());
        }
        for (int i = 0; i < numbers.length; i++) {
            Assert.assertEquals(numbers[i], DirectSimpleChunk.getNumbers(chunk, i));
            Assert.assertEquals(numbers[i], DirectSimpleChunk.getNumbers(address, i));
            try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
                Assert.assertEquals(numbers[i], simpleChunk.getNumbers(i));
            }
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testParent(final DXRAM p_instance) {
        final long chunk = DirectSimpleChunk.create();
        final long address = DirectSimpleChunk.getAddress(chunk);
        final long parent1 = DirectSimpleChunk.create();
        final long parent2 = DirectSimpleChunk.create();

        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, simpleChunk.getParentSimpleChunkID());
        }

        DirectSimpleChunk.setParentSimpleChunkID(chunk, parent1);
        Assert.assertEquals(parent1, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(parent1, DirectSimpleChunk.getParentSimpleChunkID(address));
        Assert.assertNotEquals(Address.INVALID, DirectSimpleChunk.getParentSimpleChunkID(address));

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(parent1, simpleChunk.getParentSimpleChunkID());
        }

        DirectSimpleChunk.setParentSimpleChunkID(chunk, ChunkID.INVALID_ID);
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(address));

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, simpleChunk.getParentSimpleChunkID());
        }

        DirectSimpleChunk.setParentSimpleChunkID(address, parent2);
        Assert.assertEquals(parent2, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(parent2, DirectSimpleChunk.getParentSimpleChunkID(address));

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(parent2, simpleChunk.getParentSimpleChunkID());
        }

        DirectSimpleChunk.setParentSimpleChunkID(address, ChunkID.INVALID_ID);
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, simpleChunk.getParentSimpleChunkID());
        }

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            simpleChunk.setParentSimpleChunkID(parent1);
        }
        Assert.assertEquals(parent1, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(parent1, DirectSimpleChunk.getParentSimpleChunkID(address));

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(parent1, simpleChunk.getParentSimpleChunkID());
        }

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)) {
            simpleChunk.setParentSimpleChunkID(ChunkID.INVALID_ID);
        }
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(chunk));
        Assert.assertEquals(ChunkID.INVALID_ID, DirectSimpleChunk.getParentSimpleChunkID(address));
        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(ChunkID.INVALID_ID, simpleChunk.getParentSimpleChunkID());
        }

        Assert.assertNotEquals(DirectSimpleChunk.getAddress(parent1), DirectSimpleChunk.getAddress(parent2));
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testChildren(final DXRAM p_instance) {
        final long chunk = DirectSimpleChunk.create();
        final long address = DirectSimpleChunk.getAddress(chunk);
        final long[] children = DirectSimpleChunk.create(100);

        DirectSimpleChunk.setChildrenSimpleChunkIDs(address, children);
        Assert.assertEquals(children.length, DirectSimpleChunk.getChildrenSimpleChunkLength(chunk));
        Assert.assertEquals(children.length, DirectSimpleChunk.getChildrenSimpleChunkLength(address));
        Assert.assertArrayEquals(children, DirectSimpleChunk.getChildrenSimpleChunkIDs(chunk));
        Assert.assertArrayEquals(children, DirectSimpleChunk.getChildrenSimpleChunkIDs(address));

        try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
            Assert.assertEquals(children.length, simpleChunk.getChildrenSimpleChunkLength());
            Assert.assertArrayEquals(children, simpleChunk.getChildrenSimpleChunkIDs());
        }
        for (int i = 0; i < children.length; i++) {
            Assert.assertEquals(children[i], DirectSimpleChunk.getChildrenSimpleChunkID(chunk, i));
            Assert.assertEquals(children[i], DirectSimpleChunk.getChildrenSimpleChunkID(address, i));
            try (DirectSimpleChunk simpleChunk = DirectSimpleChunk.use(chunk)){
                Assert.assertEquals(children[i], simpleChunk.getChildrenSimpleChunkID(i));
            }
        }
    }
}
