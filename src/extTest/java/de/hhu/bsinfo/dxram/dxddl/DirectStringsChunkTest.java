package de.hhu.bsinfo.dxram.dxddl;

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
public class DirectStringsChunkTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        DirectStringsChunk.init(
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testStrings(final DXRAM p_instance) {
        final long chunk = DirectStringsChunk.create();
        final long address = DirectStringsChunk.getAddress(chunk);
        final String test1 = "test1";
        final String test2 = "test2";

        Assert.assertNull(DirectStringsChunk.getS1(chunk));
        Assert.assertNull(DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertNull(DirectStringsChunk.getS2(chunk));
        Assert.assertNull(DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertNull(stringsChunk.getS1());
            Assert.assertNull(stringsChunk.getS2());
        }

        DirectStringsChunk.setS1(chunk, test1);
        DirectStringsChunk.setS2(chunk, test2);
        Assert.assertEquals(test1, DirectStringsChunk.getS1(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test2, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test2, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals(test1, stringsChunk.getS1());
            Assert.assertEquals(test2, stringsChunk.getS2());
        }

        DirectStringsChunk.setS1(chunk, test2);
        DirectStringsChunk.setS2(chunk, test1);
        Assert.assertEquals(test2, DirectStringsChunk.getS1(chunk));
        Assert.assertEquals(test2, DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test1, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals(test2, stringsChunk.getS1());
            Assert.assertEquals(test1, stringsChunk.getS2());
        }

        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            stringsChunk.setS1(test1);
            stringsChunk.setS2(test2);
        }
        Assert.assertEquals(test1, DirectStringsChunk.getS1(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test2, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test2, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals(test1, stringsChunk.getS1());
            Assert.assertEquals(test2, stringsChunk.getS2());
        }

        DirectStringsChunk.setS1(chunk, "");
        DirectStringsChunk.setS2(chunk, test1);
        Assert.assertEquals("", DirectStringsChunk.getS1(chunk));
        Assert.assertEquals("", DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test1, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals("", stringsChunk.getS1());
            Assert.assertEquals(test1, stringsChunk.getS2());
        }

        DirectStringsChunk.setS1(chunk, "");
        DirectStringsChunk.setS2(chunk, null);
        Assert.assertEquals("", DirectStringsChunk.getS1(chunk));
        Assert.assertEquals("", DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertNull(DirectStringsChunk.getS2(chunk));
        Assert.assertNull(DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals("", stringsChunk.getS1());
            Assert.assertNull(stringsChunk.getS2());
        }

        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            stringsChunk.setS1(null);
            stringsChunk.setS2("");
        }
        Assert.assertNull(DirectStringsChunk.getS1(chunk));
        Assert.assertNull(DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals("", DirectStringsChunk.getS2(chunk));
        Assert.assertEquals("", DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertNull(stringsChunk.getS1());
            Assert.assertEquals("", stringsChunk.getS2());
        }

        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            stringsChunk.setS1(test1);
            stringsChunk.setS2(test2);
        }
        Assert.assertEquals(test1, DirectStringsChunk.getS1(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test2, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test2, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals(test1, stringsChunk.getS1());
            Assert.assertEquals(test2, stringsChunk.getS2());
        }

        DirectStringsChunk.setS1(chunk, test2);
        DirectStringsChunk.setS2(chunk, test1);
        Assert.assertEquals(test2, DirectStringsChunk.getS1(chunk));
        Assert.assertEquals(test2, DirectStringsChunk.getS1ViaAddress(address));
        Assert.assertEquals(test1, DirectStringsChunk.getS2(chunk));
        Assert.assertEquals(test1, DirectStringsChunk.getS2ViaAddress(address));
        try (DirectStringsChunk stringsChunk = DirectStringsChunk.use(chunk)){
            Assert.assertEquals(test2, stringsChunk.getS1());
            Assert.assertEquals(test1, stringsChunk.getS2());
        }
    }
}
