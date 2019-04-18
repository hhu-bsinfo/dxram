package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

final class DirectTestStruct1 {

    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private static final int OFFSET_B = 0;
    private static final int OFFSET_C_LENGTH = 1;
    private static final int OFFSET_C_CID = 5;
    private static final int OFFSET_C_ADDR = 13;
    private static final int OFFSET_S_LENGTH = 21;
    private static final int OFFSET_S_CID = 25;
    private static final int OFFSET_S_ADDR = 33;
    private static final int OFFSET_I = 41;
    private static final int OFFSET_D_LENGTH = 45;
    private static final int OFFSET_D_CID = 49;
    private static final int OFFSET_D_ADDR = 57;
    static final int SIZE = 65;

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
        RAWWRITE.writeLong(p_addr, OFFSET_C_CID, -1);
        RAWWRITE.writeLong(p_addr, OFFSET_S_CID, -1);
        RAWWRITE.writeLong(p_addr, OFFSET_D_CID, -1);
        RAWWRITE.writeInt(p_addr, OFFSET_C_LENGTH, 0);
        RAWWRITE.writeInt(p_addr, OFFSET_S_LENGTH, 0);
        RAWWRITE.writeInt(p_addr, OFFSET_D_LENGTH, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_C_ADDR, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_S_ADDR, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_D_ADDR, 0);
    }

    static void remove(final long p_addr) {
        final long cid_OFFSET_C_CID = RAWREAD.readLong(p_addr, OFFSET_C_CID);
        PINNING.unpinCID(cid_OFFSET_C_CID);
        REMOVE.remove(cid_OFFSET_C_CID);
        final long cid_OFFSET_S_CID = RAWREAD.readLong(p_addr, OFFSET_S_CID);
        PINNING.unpinCID(cid_OFFSET_S_CID);
        REMOVE.remove(cid_OFFSET_S_CID);
        final long cid_OFFSET_D_CID = RAWREAD.readLong(p_addr, OFFSET_D_CID);
        PINNING.unpinCID(cid_OFFSET_D_CID);
        REMOVE.remove(cid_OFFSET_D_CID);
    }

    static boolean getB(final long p_addr) {
        return RAWREAD.readBoolean(p_addr, OFFSET_B);
    }

    static void setB(final long p_addr, final boolean p_b) {
        RAWWRITE.writeBoolean(p_addr, OFFSET_B, p_b);
    }

    static int getCLength(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_C_LENGTH);
    }

    static char getC(final long p_addr, final int index) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_C_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_C_ADDR);
        return RAWREAD.readChar(addr2, (2 * index));
    }

    static char[] getC(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_C_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readCharArray(RAWREAD.readLong(p_addr, OFFSET_C_ADDR), 0, len);
    }

    static void setC(final long p_addr, final int index, final char value) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_C_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_C_ADDR);
        RAWWRITE.writeChar(addr2, (2 * index), value);
    }

    static void setC(final long p_addr, final char[] p_c) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_C_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_C_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_C_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_C_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_C_LENGTH, 0);
        }

        if (p_c == null || p_c.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (2 * p_c.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_C_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_C_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_C_LENGTH, p_c.length);
        RAWWRITE.writeCharArray(addr2, 0, p_c);
    }

    static String getS(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_S_ADDR), 0, len));
    }

    static void setS(final long p_addr, final String p_s) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_S_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_S_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_S_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_S_LENGTH, (p_s == null ? -1 : 0));
        }

        if (p_s == null || p_s.length() == 0) {
            return;
        }

        final byte[] str = p_s.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_S_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_S_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_S_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    static int getI(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_I);
    }

    static void setI(final long p_addr, final int p_i) {
        RAWWRITE.writeInt(p_addr, OFFSET_I, p_i);
    }

    static int getDLength(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_D_LENGTH);
    }

    static double getD(final long p_addr, final int index) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_D_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_D_ADDR);
        return RAWREAD.readDouble(addr2, (8 * index));
    }

    static double[] getD(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_D_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readDoubleArray(RAWREAD.readLong(p_addr, OFFSET_D_ADDR), 0, len);
    }

    static void setD(final long p_addr, final int index, final double value) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_D_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_D_ADDR);
        RAWWRITE.writeDouble(addr2, (8 * index), value);
    }

    static void setD(final long p_addr, final double[] p_d) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_D_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_D_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_D_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_D_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_D_LENGTH, 0);
        }

        if (p_d == null || p_d.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_d.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_D_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_D_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_D_LENGTH, p_d.length);
        RAWWRITE.writeDoubleArray(addr2, 0, p_d);
    }
}
