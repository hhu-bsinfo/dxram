package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectPrimitiveDataTypesChunk implements AutoCloseable {

    private static final int OFFSET_B = 0;
    private static final int OFFSET_B2 = 1;
    private static final int OFFSET_C = 2;
    private static final int OFFSET_D = 4;
    private static final int OFFSET_F = 12;
    private static final int OFFSET_NUM = 16;
    private static final int OFFSET_BIGNUM = 20;
    private static final int OFFSET_S = 28;
    private static final int SIZE = 30;
    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private long m_addr = 0;

    public static int size() {
        return SIZE;
    }

    public static void init(
            final CreateLocal create, 
            final CreateReservedLocal create_reserved, 
            final ReserveLocal reserve, 
            final Remove remove, 
            final PinningLocal pinning, 
            final RawReadLocal rawread, 
            final RawWriteLocal rawwrite) {
        if (!INITIALIZED) {
            INITIALIZED = true;
            CREATE = create;
            CREATE_RESERVED = create_reserved;
            RESERVE = reserve;
            REMOVE = remove;
            PINNING = pinning;
            RAWREAD = rawread;
            RAWWRITE = rawwrite;
        }
    }

    public static long getAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return PINNING.translate(p_cid);
    }

    public static long[] getAddresses(final long[] p_cids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] addresses = new long[p_cids.length];
        for (int i = 0; i < p_cids.length; i ++) {
            addresses[i] = PINNING.translate(p_cids[i]);
        }
        return addresses;
    }

    public static long create() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] cids = new long[1];
        CREATE.create(cids, 1, SIZE);
        final long addr = PINNING.pin(cids[0]).getAddress();
        return cids[0];
    }

    public static long[] create(final int p_count) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] cids = new long[p_count];
        CREATE.create(cids, p_count, SIZE);

        for (int i = 0; i < p_count; i ++) {
            final long addr = PINNING.pin(cids[i]).getAddress();
        }

        return cids;
    }

    public static void createReserved(final long[] p_reserved_cids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int[] sizes = new int[p_reserved_cids.length];
        for (int i = 0; i < p_reserved_cids.length; i ++) {
            sizes[i] = SIZE;
        }
        CREATE_RESERVED.create(p_reserved_cids, p_reserved_cids.length, sizes);

        for (int i = 0; i < p_reserved_cids.length; i ++) {
            final long addr = PINNING.pin(p_reserved_cids[i]).getAddress();
        }
    }

    public static void remove(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        PINNING.unpinCID(p_cid);
        REMOVE.remove(p_cid);
    }

    public static void remove(final long[] p_cids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        for (int i = 0; i < p_cids.length; i ++) {
            final long addr = PINNING.translate(p_cids[i]);
            PINNING.unpinCID(p_cids[i]);
            REMOVE.remove(p_cids[i]);
        }
    }

    public static boolean getB(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readBoolean(addr, OFFSET_B);
    }

    public static boolean getBViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readBoolean(p_addr, OFFSET_B);
    }

    public static void setB(final long p_cid, final boolean p_b) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeBoolean(addr, OFFSET_B, p_b);
    }

    public static void setBViaAddress(final long p_addr, final boolean p_b) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeBoolean(p_addr, OFFSET_B, p_b);
    }

    public static byte getB2(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readByte(addr, OFFSET_B2);
    }

    public static byte getB2ViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readByte(p_addr, OFFSET_B2);
    }

    public static void setB2(final long p_cid, final byte p_b2) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeByte(addr, OFFSET_B2, p_b2);
    }

    public static void setB2ViaAddress(final long p_addr, final byte p_b2) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeByte(p_addr, OFFSET_B2, p_b2);
    }

    public static char getC(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readChar(addr, OFFSET_C);
    }

    public static char getCViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readChar(p_addr, OFFSET_C);
    }

    public static void setC(final long p_cid, final char p_c) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeChar(addr, OFFSET_C, p_c);
    }

    public static void setCViaAddress(final long p_addr, final char p_c) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeChar(p_addr, OFFSET_C, p_c);
    }

    public static double getD(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readDouble(addr, OFFSET_D);
    }

    public static double getDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readDouble(p_addr, OFFSET_D);
    }

    public static void setD(final long p_cid, final double p_d) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeDouble(addr, OFFSET_D, p_d);
    }

    public static void setDViaAddress(final long p_addr, final double p_d) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeDouble(p_addr, OFFSET_D, p_d);
    }

    public static float getF(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readFloat(addr, OFFSET_F);
    }

    public static float getFViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readFloat(p_addr, OFFSET_F);
    }

    public static void setF(final long p_cid, final float p_f) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeFloat(addr, OFFSET_F, p_f);
    }

    public static void setFViaAddress(final long p_addr, final float p_f) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeFloat(p_addr, OFFSET_F, p_f);
    }

    public static int getNum(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_NUM);
    }

    public static int getNumViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_NUM);
    }

    public static void setNum(final long p_cid, final int p_num) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_NUM, p_num);
    }

    public static void setNumViaAddress(final long p_addr, final int p_num) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_NUM, p_num);
    }

    public static long getBignum(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_BIGNUM);
    }

    public static long getBignumViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_BIGNUM);
    }

    public static void setBignum(final long p_cid, final long p_bignum) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeLong(addr, OFFSET_BIGNUM, p_bignum);
    }

    public static void setBignumViaAddress(final long p_addr, final long p_bignum) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(p_addr, OFFSET_BIGNUM, p_bignum);
    }

    public static short getS(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readShort(addr, OFFSET_S);
    }

    public static short getSViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readShort(p_addr, OFFSET_S);
    }

    public static void setS(final long p_cid, final short p_s) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeShort(addr, OFFSET_S, p_s);
    }

    public static void setSViaAddress(final long p_addr, final short p_s) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeShort(p_addr, OFFSET_S, p_s);
    }

    private DirectPrimitiveDataTypesChunk() {}

    public static DirectPrimitiveDataTypesChunk use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        DirectPrimitiveDataTypesChunk tmp = new DirectPrimitiveDataTypesChunk();
        tmp.m_addr = PINNING.translate(p_cid);
        return tmp;
    }

    public boolean getB() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readBoolean(m_addr, OFFSET_B);
    }

    public void setB(final boolean p_b) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeBoolean(m_addr, OFFSET_B, p_b);
    }

    public byte getB2() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readByte(m_addr, OFFSET_B2);
    }

    public void setB2(final byte p_b2) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeByte(m_addr, OFFSET_B2, p_b2);
    }

    public char getC() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readChar(m_addr, OFFSET_C);
    }

    public void setC(final char p_c) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeChar(m_addr, OFFSET_C, p_c);
    }

    public double getD() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readDouble(m_addr, OFFSET_D);
    }

    public void setD(final double p_d) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeDouble(m_addr, OFFSET_D, p_d);
    }

    public float getF() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readFloat(m_addr, OFFSET_F);
    }

    public void setF(final float p_f) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeFloat(m_addr, OFFSET_F, p_f);
    }

    public int getNum() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_NUM);
    }

    public void setNum(final int p_num) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_NUM, p_num);
    }

    public long getBignum() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_BIGNUM);
    }

    public void setBignum(final long p_bignum) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(m_addr, OFFSET_BIGNUM, p_bignum);
    }

    public short getS() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readShort(m_addr, OFFSET_S);
    }

    public void setS(final short p_s) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeShort(m_addr, OFFSET_S, p_s);
    }

    @Override
    public void close() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        m_addr = 0;
    }
}
