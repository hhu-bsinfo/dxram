package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

final class DirectPrimitiveDataTypesStruct {

    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private static final int OFFSET_B = 0;
    private static final int OFFSET_B2 = 1;
    private static final int OFFSET_C = 2;
    private static final int OFFSET_D = 4;
    private static final int OFFSET_F = 12;
    private static final int OFFSET_NUM = 16;
    private static final int OFFSET_BIGNUM = 20;
    private static final int OFFSET_S = 28;
    static final int SIZE = 30;

    static void init(
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

    static void create(final long p_addr) {
    }

    static void remove(final long p_addr) {
    }

    static boolean getB(final long p_addr) {
        return RAWREAD.readBoolean(p_addr, OFFSET_B);
    }

    static void setB(final long p_addr, final boolean p_b) {
        RAWWRITE.writeBoolean(p_addr, OFFSET_B, p_b);
    }

    static byte getB2(final long p_addr) {
        return RAWREAD.readByte(p_addr, OFFSET_B2);
    }

    static void setB2(final long p_addr, final byte p_b2) {
        RAWWRITE.writeByte(p_addr, OFFSET_B2, p_b2);
    }

    static char getC(final long p_addr) {
        return RAWREAD.readChar(p_addr, OFFSET_C);
    }

    static void setC(final long p_addr, final char p_c) {
        RAWWRITE.writeChar(p_addr, OFFSET_C, p_c);
    }

    static double getD(final long p_addr) {
        return RAWREAD.readDouble(p_addr, OFFSET_D);
    }

    static void setD(final long p_addr, final double p_d) {
        RAWWRITE.writeDouble(p_addr, OFFSET_D, p_d);
    }

    static float getF(final long p_addr) {
        return RAWREAD.readFloat(p_addr, OFFSET_F);
    }

    static void setF(final long p_addr, final float p_f) {
        RAWWRITE.writeFloat(p_addr, OFFSET_F, p_f);
    }

    static int getNum(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_NUM);
    }

    static void setNum(final long p_addr, final int p_num) {
        RAWWRITE.writeInt(p_addr, OFFSET_NUM, p_num);
    }

    static long getBignum(final long p_addr) {
        return RAWREAD.readLong(p_addr, OFFSET_BIGNUM);
    }

    static void setBignum(final long p_addr, final long p_bignum) {
        RAWWRITE.writeLong(p_addr, OFFSET_BIGNUM, p_bignum);
    }

    static short getS(final long p_addr) {
        return RAWREAD.readShort(p_addr, OFFSET_S);
    }

    static void setS(final long p_addr, final short p_s) {
        RAWWRITE.writeShort(p_addr, OFFSET_S, p_s);
    }
}
