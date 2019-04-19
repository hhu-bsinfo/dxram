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
public class DirectComplexChunkTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        DirectComplexChunk.init(
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testStructsInChunk(final DXRAM p_instance) {
        final long chunk = DirectComplexChunk.create();
        final long address = DirectComplexChunk.getAddress(chunk);

        boolean b = true;
        char[] c = new char[] { 'a', 'b', 'c' };
        String s = "a string";
        int i = 42;
        double[] d = new double[] { 3.14, 2.5, 1.66 };

        // TestStruct1
        DirectComplexChunk.test1.setB(chunk, b);
        DirectComplexChunk.test1.setCViaAddress(address, c);
        DirectComplexChunk.test1.setS(chunk, s);
        try (DirectComplexChunk complexChunk = DirectComplexChunk.use(chunk)) {
            complexChunk.test1().setI(i);
            complexChunk.test1().setD(d);
        }
        Assert.assertEquals(b, DirectComplexChunk.test1.getB(chunk));
        Assert.assertEquals(b, DirectComplexChunk.test1.getBViaAddress(address));
        Assert.assertArrayEquals(c, DirectComplexChunk.test1.getC(chunk));
        Assert.assertArrayEquals(c, DirectComplexChunk.test1.getCViaAddress(address));
        Assert.assertEquals(s, DirectComplexChunk.test1.getS(chunk));
        Assert.assertEquals(s, DirectComplexChunk.test1.getSViaAddress(address));
        Assert.assertEquals(i, DirectComplexChunk.test1.getI(chunk));
        Assert.assertEquals(i, DirectComplexChunk.test1.getIViaAddress(address));
        Assert.assertArrayEquals(d, DirectComplexChunk.test1.getD(chunk), Double.MIN_VALUE);
        Assert.assertArrayEquals(d, DirectComplexChunk.test1.getDViaAddress(address), Double.MIN_VALUE);
        try (DirectComplexChunk complexChunk = DirectComplexChunk.use(chunk)) {
            Assert.assertEquals(b, complexChunk.test1().getB());
            Assert.assertArrayEquals(c, complexChunk.test1().getC());
            Assert.assertEquals(s, complexChunk.test1().getS());
            Assert.assertEquals(i, complexChunk.test1().getI());
            Assert.assertArrayEquals(d, complexChunk.test1().getD(), Double.MIN_VALUE);
        }
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testEnumCache(final DXRAM p_instance) {
        final long chunk = DirectComplexChunk.create();
        final long address = DirectComplexChunk.getAddress(chunk);

        Assert.assertEquals(Weekday.Monday, DirectComplexChunk.getDay(chunk));
        Assert.assertEquals(Month.January, DirectComplexChunk.getMonth(chunk));
        Assert.assertEquals(OS.Windows, DirectComplexChunk.getMyOS(chunk));
        Assert.assertEquals(ProgrammingLanguage.B, DirectComplexChunk.getFavoriteLang(chunk));
        Assert.assertEquals(HotDrink.Tea, DirectComplexChunk.getFavoriteDrink(chunk));
        Assert.assertEquals(Weekday.Monday, DirectComplexChunk.getDayViaAddress(address));
        Assert.assertEquals(Month.January, DirectComplexChunk.getMonthViaAddress(address));
        Assert.assertEquals(OS.Windows, DirectComplexChunk.getMyOSViaAddress(address));
        Assert.assertEquals(ProgrammingLanguage.B, DirectComplexChunk.getFavoriteLangViaAddress(address));
        Assert.assertEquals(HotDrink.Tea, DirectComplexChunk.getFavoriteDrinkViaAddress(address));
        try (DirectComplexChunk complexChunk = DirectComplexChunk.use(chunk)) {
            Assert.assertEquals(Weekday.Monday, complexChunk.getDay());
            Assert.assertEquals(Month.January, complexChunk.getMonth());
            Assert.assertEquals(OS.Windows, complexChunk.getMyOS());
            Assert.assertEquals(ProgrammingLanguage.B, complexChunk.getFavoriteLang());
            Assert.assertEquals(HotDrink.Tea, complexChunk.getFavoriteDrink());
        }
    }
}
