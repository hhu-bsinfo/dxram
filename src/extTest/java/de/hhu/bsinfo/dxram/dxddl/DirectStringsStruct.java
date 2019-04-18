package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

final class DirectStringsStruct {

    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private static final int OFFSET_S1_LENGTH = 0;
    private static final int OFFSET_S1_CID = 4;
    private static final int OFFSET_S1_ADDR = 12;
    private static final int OFFSET_S2_LENGTH = 20;
    private static final int OFFSET_S2_CID = 24;
    private static final int OFFSET_S2_ADDR = 32;
    static final int SIZE = 40;

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
        RAWWRITE.writeLong(p_addr, OFFSET_S1_CID, -1);
        RAWWRITE.writeLong(p_addr, OFFSET_S2_CID, -1);
        RAWWRITE.writeInt(p_addr, OFFSET_S1_LENGTH, 0);
        RAWWRITE.writeInt(p_addr, OFFSET_S2_LENGTH, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_S1_ADDR, 0);
        RAWWRITE.writeLong(p_addr, OFFSET_S2_ADDR, 0);
    }

    static void remove(final long p_addr) {
        final long cid_OFFSET_S1_CID = RAWREAD.readLong(p_addr, OFFSET_S1_CID);
        PINNING.unpinCID(cid_OFFSET_S1_CID);
        REMOVE.remove(cid_OFFSET_S1_CID);
        final long cid_OFFSET_S2_CID = RAWREAD.readLong(p_addr, OFFSET_S2_CID);
        PINNING.unpinCID(cid_OFFSET_S2_CID);
        REMOVE.remove(cid_OFFSET_S2_CID);
    }

    static String getS1(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S1_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_S1_ADDR), 0, len));
    }

    static void setS1(final long p_addr, final String p_s1) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S1_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_S1_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_S1_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_S1_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_S1_LENGTH, (p_s1 == null ? -1 : 0));
        }

        if (p_s1 == null || p_s1.length() == 0) {
            return;
        }

        final byte[] str = p_s1.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_S1_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_S1_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_S1_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    static String getS2(final long p_addr) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S2_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_S2_ADDR), 0, len));
    }

    static void setS2(final long p_addr, final String p_s2) {
        final int len = RAWREAD.readInt(p_addr, OFFSET_S2_LENGTH);
        final long cid = RAWREAD.readLong(p_addr, OFFSET_S2_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(p_addr, OFFSET_S2_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_S2_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_S2_LENGTH, (p_s2 == null ? -1 : 0));
        }

        if (p_s2 == null || p_s2.length() == 0) {
            return;
        }

        final byte[] str = p_s2.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_S2_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_S2_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_S2_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }
}
