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
public class DirectPrimitiveDataTypesChunkTest {

    @BeforeTestInstance(runOnNodeIdx = 1)
    public void initTests(final DXRAM p_instance) {
        ChunkLocalService chunkLocalService = p_instance.getService(ChunkLocalService.class);
        ChunkService chunkService = p_instance.getService(ChunkService.class);

        DirectPrimitiveDataTypesChunk.init(
                chunkLocalService.createLocal(),
                chunkLocalService.createReservedLocal(),
                chunkLocalService.reserveLocal(),
                chunkService.remove(),
                chunkLocalService.pinningLocal(),
                chunkLocalService.rawReadLocal(),
                chunkLocalService.rawWriteLocal());
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testStatic(final DXRAM p_instance) {
        final long chunk = DirectPrimitiveDataTypesChunk.create();
        final long address = DirectPrimitiveDataTypesChunk.getAddress(chunk);

        boolean b = true;
        byte b2 = 42;
        char c = 'c';
        double d = .00478;
        float f = 3424.3484f;
        int num = 73264;
        long bignum = 873265743223L;
        short s = 233;

        DirectPrimitiveDataTypesChunk.setB(chunk, b);
        DirectPrimitiveDataTypesChunk.setB2(chunk, b2);
        DirectPrimitiveDataTypesChunk.setC(chunk, c);
        DirectPrimitiveDataTypesChunk.setD(chunk, d);
        DirectPrimitiveDataTypesChunk.setF(chunk, f);
        DirectPrimitiveDataTypesChunk.setNum(chunk, num);
        DirectPrimitiveDataTypesChunk.setBignum(chunk, bignum);
        DirectPrimitiveDataTypesChunk.setS(chunk, s);

        check(chunk, address, b, b2, c, d, f, num, bignum, s);
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testStaticViaAddress(final DXRAM p_instance) {
        final long chunk = DirectPrimitiveDataTypesChunk.create();
        final long address = DirectPrimitiveDataTypesChunk.getAddress(chunk);

        boolean b = false;
        byte b2 = 43;
        char c = 'd';
        double d = .00378;
        float f = 7424.3584f;
        int num = 7433264;
        long bignum = 9923555743223L;
        short s = 323;

        DirectPrimitiveDataTypesChunk.setBViaAddress(address, b);
        DirectPrimitiveDataTypesChunk.setB2ViaAddress(address, b2);
        DirectPrimitiveDataTypesChunk.setCViaAddress(address, c);
        DirectPrimitiveDataTypesChunk.setDViaAddress(address, d);
        DirectPrimitiveDataTypesChunk.setFViaAddress(address, f);
        DirectPrimitiveDataTypesChunk.setNumViaAddress(address, num);
        DirectPrimitiveDataTypesChunk.setBignumViaAddress(address, bignum);
        DirectPrimitiveDataTypesChunk.setSViaAddress(address, s);

        check(chunk, address, b, b2, c, d, f, num, bignum, s);
    }

    @TestInstance(runOnNodeIdx = 1)
    public void testObj(final DXRAM p_instance) {
        final long chunk = DirectPrimitiveDataTypesChunk.create();
        final long address = DirectPrimitiveDataTypesChunk.getAddress(chunk);

        boolean b = true;
        byte b2 = 52;
        char c = 'e';
        double d = .22478;
        float f = 23464.3784f;
        int num = 7232564;
        long bignum = 9874398327855L;
        short s = 554;

        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            primitiveDataTypesChunk.setB(b);
            primitiveDataTypesChunk.setB2(b2);
            primitiveDataTypesChunk.setC(c);
            primitiveDataTypesChunk.setD(d);
            primitiveDataTypesChunk.setF(f);
            primitiveDataTypesChunk.setNum(num);
            primitiveDataTypesChunk.setBignum(bignum);
            primitiveDataTypesChunk.setS(s);
        }

        check(chunk, address, b, b2, c, d, f, num, bignum, s);
    }

    private void check(
            long chunk,
            long address,
            boolean b,
            byte b2,
            char c,
            double d,
            float f,
            int num,
            long bignum,
            short s) {
        Assert.assertEquals(b, DirectPrimitiveDataTypesChunk.getB(chunk));
        Assert.assertEquals(b, DirectPrimitiveDataTypesChunk.getBViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(b, primitiveDataTypesChunk.getB());
        }
        Assert.assertEquals(b2, DirectPrimitiveDataTypesChunk.getB2(chunk));
        Assert.assertEquals(b2, DirectPrimitiveDataTypesChunk.getB2ViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(b2, primitiveDataTypesChunk.getB2());
        }
        Assert.assertEquals(c, DirectPrimitiveDataTypesChunk.getC(chunk));
        Assert.assertEquals(c, DirectPrimitiveDataTypesChunk.getCViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(c, primitiveDataTypesChunk.getC());
        }
        Assert.assertEquals(d, DirectPrimitiveDataTypesChunk.getD(chunk), Double.MIN_VALUE);
        Assert.assertEquals(d, DirectPrimitiveDataTypesChunk.getDViaAddress(address), Double.MIN_VALUE);
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(d, primitiveDataTypesChunk.getD(), Double.MIN_VALUE);
        }
        Assert.assertEquals(f, DirectPrimitiveDataTypesChunk.getF(chunk), Float.MIN_VALUE);
        Assert.assertEquals(f, DirectPrimitiveDataTypesChunk.getFViaAddress(address), Float.MIN_VALUE);
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(f, primitiveDataTypesChunk.getF(), Float.MIN_VALUE);
        }
        Assert.assertEquals(num, DirectPrimitiveDataTypesChunk.getNum(chunk));
        Assert.assertEquals(num, DirectPrimitiveDataTypesChunk.getNumViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(num, primitiveDataTypesChunk.getNum());
        }
        Assert.assertEquals(bignum, DirectPrimitiveDataTypesChunk.getBignum(chunk));
        Assert.assertEquals(bignum, DirectPrimitiveDataTypesChunk.getBignumViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(bignum, primitiveDataTypesChunk.getBignum());
        }
        Assert.assertEquals(s, DirectPrimitiveDataTypesChunk.getS(chunk));
        Assert.assertEquals(s, DirectPrimitiveDataTypesChunk.getSViaAddress(address));
        try (DirectPrimitiveDataTypesChunk primitiveDataTypesChunk = DirectPrimitiveDataTypesChunk.use(chunk)){
            Assert.assertEquals(s, primitiveDataTypesChunk.getS());
        }
    }
}
