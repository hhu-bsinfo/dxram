package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

final class DirectTestStruct2 {

    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private static final int OFFSET_S_LENGTH = 0;
    private static final int OFFSET_S_CID = 4;
    private static final int OFFSET_S_ADDR = 12;
    private static final int OFFSET_NUM_LENGTH = 20;
    private static final int OFFSET_NUM_CID = 24;
    private static final int OFFSET_NUM_ADDR = 32;
    private static final int OFFSET_X1 = 40;
    private static final int OFFSET_X2 = 42;
    private static final int OFFSET_ALPHA_LENGTH = 44;
    private static final int OFFSET_ALPHA_CID = 48;
    private static final int OFFSET_ALPHA_ADDR = 56;
    private static final int OFFSET_Y1 = 64;
    private static final int OFFSET_Y2 = 66;
    static final int SIZE = 68;

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
        RAWWRITE.writeLong(p_addr, OFFSET_S_CID, -1);
        RAWWRITE.writeLong(p_addr, OFFSET_NUM_CID, -1);
        RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_CID, -1);
        RAWWRITE.writeInt(p_addr, OFFSET_S_LENGTH, 0);
        RAWWRITE.writeInt(p_addr, OFFSET_NUM_LENGTH, 0);
        RAWWRITE.writeInt(p_addr, OFFSET_ALPHA_LENGTH, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_S_ADDR, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_NUM_ADDR, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_ADDR, 0);
    }

    static void remove(final long p_addr) {
        final long cid_OFFSET_S_CID = RAWREAD.readLong(p_addr, OFFSET_S_CID);
        PINNING.unpinCID(cid_OFFSET_S_CID);
        REMOVE.remove(cid_OFFSET_S_CID);
        final long cid_OFFSET_NUM_CID = RAWREAD.readLong(p_addr, OFFSET_NUM_CID);
        PINNING.unpinCID(cid_OFFSET_NUM_CID);
        REMOVE.remove(cid_OFFSET_NUM_CID);
        final long cid_OFFSET_ALPHA_CID = RAWREAD.readLong(p_addr, OFFSET_ALPHA_CID);
        PINNING.unpinCID(cid_OFFSET_ALPHA_CID);
        REMOVE.remove(cid_OFFSET_ALPHA_CID);
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

    static int getNumLength(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_NUM_LENGTH);
    }

    static int getNum(final long p_addr, final int index) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_NUM_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_NUM_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    static int[] getNum(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_NUM_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(p_addr, OFFSET_NUM_ADDR), 0, len);
    }

    static void setNum(final long p_addr, final int index, final int value) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_NUM_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_NUM_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    static void setNum(final long p_addr, final int[] p_num) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_NUM_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_NUM_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_NUM_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_NUM_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_NUM_LENGTH, 0);
        }

        if (p_num == null || p_num.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_num.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_NUM_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_NUM_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_NUM_LENGTH, p_num.length);
        RAWWRITE.writeIntArray(addr2, 0, p_num);
    }

    static short getX1(final long p_addr) {
        return RAWREAD.readShort(p_addr, OFFSET_X1);
    }

    static void setX1(final long p_addr, final short p_x1) {
        RAWWRITE.writeShort(p_addr, OFFSET_X1, p_x1);
    }

    static short getX2(final long p_addr) {
        return RAWREAD.readShort(p_addr, OFFSET_X2);
    }

    static void setX2(final long p_addr, final short p_x2) {
        RAWWRITE.writeShort(p_addr, OFFSET_X2, p_x2);
    }

    static int getAlphaLength(final long p_addr) {
        return RAWREAD.readInt(p_addr, OFFSET_ALPHA_LENGTH);
    }

    static char getAlpha(final long p_addr, final int index) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_ALPHA_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_ALPHA_ADDR);
        return RAWREAD.readChar(addr2, (2 * index));
    }

    static char[] getAlpha(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_ALPHA_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readCharArray(RAWREAD.readLong(p_addr, OFFSET_ALPHA_ADDR), 0, len);
    }

    static void setAlpha(final long p_addr, final int index, final char value) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_ALPHA_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_ALPHA_ADDR);
        RAWWRITE.writeChar(addr2, (2 * index), value);
    }

    static void setAlpha(final long p_addr, final char[] p_alpha) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_ALPHA_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_ALPHA_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_ALPHA_LENGTH, 0);
        }

        if (p_alpha == null || p_alpha.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (2 * p_alpha.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_ALPHA_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_ALPHA_LENGTH, p_alpha.length);
        RAWWRITE.writeCharArray(addr2, 0, p_alpha);
    }

    static short getY1(final long p_addr) {
        return RAWREAD.readShort(p_addr, OFFSET_Y1);
    }

    static void setY1(final long p_addr, final short p_y1) {
        RAWWRITE.writeShort(p_addr, OFFSET_Y1, p_y1);
    }

    static short getY2(final long p_addr) {
        return RAWREAD.readShort(p_addr, OFFSET_Y2);
    }

    static void setY2(final long p_addr, final short p_y2) {
        RAWWRITE.writeShort(p_addr, OFFSET_Y2, p_y2);
    }
}
