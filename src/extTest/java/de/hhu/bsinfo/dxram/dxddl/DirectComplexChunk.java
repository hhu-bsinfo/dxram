package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectComplexChunk implements AutoCloseable {

    private static final int HEADER_LID = 0;
    private static final int HEADER_TYPE = 6;
    private static final int OFFSET_MISC_LENGTH = 8;
    private static final int OFFSET_MISC_CID = 12;
    private static final int OFFSET_MISC_ADDR = 20;
    private static final int OFFSET_NUM = 28;
    private static final int OFFSET_PARENT_ID = 32;
    private static final int OFFSET_TEMP = 40;
    private static final int OFFSET_ANOTHER_ID = 48;
    private static final int OFFSET_REFS_LENGTH = 56;
    private static final int OFFSET_REFS_CID = 60;
    private static final int OFFSET_REFS_ADDR = 68;
    private static final int OFFSET_TEST1_STRUCT = 76;
    private static final int OFFSET_CHILDREN_LENGTH = 141;
    private static final int OFFSET_CHILDREN_CID = 145;
    private static final int OFFSET_CHILDREN_ADDR = 153;
    private static final int OFFSET_NAME_LENGTH = 161;
    private static final int OFFSET_NAME_CID = 165;
    private static final int OFFSET_NAME_ADDR = 173;
    private static final int OFFSET_PERSON_ID = 181;
    private static final int OFFSET_DAY_ENUM = 189;
    private static final int OFFSET_TEST2_STRUCT = 193;
    private static final int OFFSET_COUNTRY_ID = 261;
    private static final int OFFSET_MONTH_ENUM = 269;
    private static final int OFFSET_CITY_ID = 273;
    private static final int OFFSET_PRIMITIVETYPES_STRUCT = 281;
    private static final int OFFSET_PRIMITIVETYPES2_ID = 311;
    private static final int OFFSET_MYOS_ENUM = 319;
    private static final int OFFSET_FAVORITEDRINK_ENUM = 323;
    private static final int OFFSET_MORESTRINGS_ID = 327;
    private static final int OFFSET_FAVORITELANG_ENUM = 335;
    private static final int OFFSET_MORESTRINGS2_STRUCT = 339;
    private static final int SIZE = 379;
    private static short TYPE = 0;
    private static boolean INITIALIZED = false;
    private static CreateLocal CREATE;
    private static CreateReservedLocal CREATE_RESERVED;
    private static ReserveLocal RESERVE;
    private static Remove REMOVE;
    private static PinningLocal PINNING;
    private static RawReadLocal RAWREAD;
    private static RawWriteLocal RAWWRITE;
    private long m_addr = 0x0L;

    public static int size() {
        return SIZE;
    }

    public static boolean isValidType(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return (addr != 0x0L) && (RAWREAD.readShort(addr, HEADER_TYPE) == TYPE);
    }

    static void setTYPE(final short p_type) {
        TYPE = p_type;
    }

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
            DirectTestStruct1.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectTestStruct2.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectPrimitiveDataTypesStruct.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectStringsStruct.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
        }
    }

    public static long getAddress(final long p_id) {
        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            return 0xFFFF000000000000L | PINNING.translate(p_id);
        }

        return p_id;
    }

    public static long[] getAddresses(final long[] p_ids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] addresses = new long[p_ids.length];
        for (int i = 0; i < p_ids.length; i ++) {
            if ((p_ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                addresses[i] = 0xFFFF000000000000L | PINNING.translate(p_ids[i]);
            } else {
                addresses[i] = p_ids[i];
            }
        }

        return addresses;
    }

    public static long getCID(final long p_id) {
        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L || p_id == 0xFFFFFFFFFFFFFFFFL) {
            return p_id;
        } else {
            return (RAWREAD.readLong(p_id & 0xFFFFFFFFFFFFL, HEADER_LID) & 0xFFFFFFFFFFFFL) | DirectAccessSecurityManager.NID;
        }
    }

    public static long[] getCIDs(final long[] p_ids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] cids = new long[p_ids.length];
        for (int i = 0; i < p_ids.length; i ++) {
            if ((p_ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L || p_ids[i] == 0xFFFFFFFFFFFFFFFFL) {
                cids[i] = p_ids[i];
            } else {
                cids[i] = (RAWREAD.readLong(p_ids[i] & 0xFFFFFFFFFFFFL, HEADER_LID) & 0xFFFFFFFFFFFFL) | DirectAccessSecurityManager.NID;
            }
        }

        return cids;
    }

    public static long[] reserve(final int p_count) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        // TODO: REMOVE THIS WHEN DXRAM BUG IS FIXED (NID set for reserved CIDs)
        final long[] cids = new long[p_count];
        RESERVE.reserve(p_count);

        for (int i = 0; i < p_count; i ++) {
            cids[i] |= DirectAccessSecurityManager.NID;
        }

        return cids;
    }

    public static long create() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] cids = new long[1];
        CREATE.create(cids, 1, SIZE);
        final long addr = PINNING.pin(cids[0]).getAddress();
        RAWWRITE.writeLong(addr, HEADER_LID, ((long) TYPE << 48) | (cids[0] & 0xFFFFFFFFFFFFL));
        RAWWRITE.writeLong(addr, OFFSET_MISC_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_REFS_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_CITY_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, 0);
        DirectTestStruct1.create(addr + OFFSET_TEST1_STRUCT);
        DirectTestStruct2.create(addr + OFFSET_TEST2_STRUCT);
        DirectPrimitiveDataTypesStruct.create(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        DirectStringsStruct.create(addr + OFFSET_MORESTRINGS2_STRUCT);
        return addr | 0xFFFF000000000000L;
    }

    public static long[] create(final int p_count) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long[] cids = new long[p_count];
        CREATE.create(cids, p_count, SIZE);

        for (int i = 0; i < p_count; i ++) {
            final long addr = PINNING.pin(cids[i]).getAddress();
            RAWWRITE.writeLong(addr, HEADER_LID, ((long) TYPE << 48) | (cids[i] & 0xFFFFFFFFFFFFL));
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_CITY_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, 0);
            DirectTestStruct1.create(addr + OFFSET_TEST1_STRUCT);
            DirectTestStruct2.create(addr + OFFSET_TEST2_STRUCT);
            DirectPrimitiveDataTypesStruct.create(addr + OFFSET_PRIMITIVETYPES_STRUCT);
            DirectStringsStruct.create(addr + OFFSET_MORESTRINGS2_STRUCT);
            cids[i] = addr | 0xFFFF000000000000L;
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
            RAWWRITE.writeLong(addr, HEADER_TYPE, ((long) TYPE << 48) | (p_reserved_cids[i] & 0xFFFFFFFFFFFFL));
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_CITY_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, 0);
            DirectTestStruct1.create(addr + OFFSET_TEST1_STRUCT);
            DirectTestStruct2.create(addr + OFFSET_TEST2_STRUCT);
            DirectPrimitiveDataTypesStruct.create(addr + OFFSET_PRIMITIVETYPES_STRUCT);
            DirectStringsStruct.create(addr + OFFSET_MORESTRINGS2_STRUCT);
        }
    }

    public static void remove(final long p_id) {
        long addr;
        long cid;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            cid = p_id;
            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
            cid = (RAWREAD.readLong(addr, HEADER_LID) & 0xFFFFFFFFFFFFL) | DirectAccessSecurityManager.NID;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long cid_OFFSET_MISC_CID = RAWREAD.readLong(addr, OFFSET_MISC_CID);
        PINNING.unpinCID(cid_OFFSET_MISC_CID);
        REMOVE.remove(cid_OFFSET_MISC_CID);
        final long cid_OFFSET_REFS_CID = RAWREAD.readLong(addr, OFFSET_REFS_CID);
        PINNING.unpinCID(cid_OFFSET_REFS_CID);
        REMOVE.remove(cid_OFFSET_REFS_CID);
        final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
        PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
        REMOVE.remove(cid_OFFSET_CHILDREN_CID);
        final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
        PINNING.unpinCID(cid_OFFSET_NAME_CID);
        REMOVE.remove(cid_OFFSET_NAME_CID);
        DirectTestStruct1.remove(addr + OFFSET_TEST1_STRUCT);
        DirectTestStruct2.remove(addr + OFFSET_TEST2_STRUCT);
        DirectPrimitiveDataTypesStruct.remove(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        DirectStringsStruct.remove(addr + OFFSET_MORESTRINGS2_STRUCT);
        PINNING.unpinCID(cid);
        REMOVE.remove(cid);
    }

    public static void remove(final long[] p_ids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        for (int i = 0; i < p_ids.length; i ++) {
            long addr;
            long cid;

            if ((p_ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_ids[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                cid = p_ids[i];
                addr = PINNING.translate(p_ids[i]);
            } else if (p_ids[i] != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_ids[i] & 0xFFFFFFFFFFFFL;
                cid = (RAWREAD.readLong(addr, HEADER_LID) & 0xFFFFFFFFFFFFL) | DirectAccessSecurityManager.NID;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            final long cid_OFFSET_MISC_CID = RAWREAD.readLong(addr, OFFSET_MISC_CID);
            PINNING.unpinCID(cid_OFFSET_MISC_CID);
            REMOVE.remove(cid_OFFSET_MISC_CID);
            final long cid_OFFSET_REFS_CID = RAWREAD.readLong(addr, OFFSET_REFS_CID);
            PINNING.unpinCID(cid_OFFSET_REFS_CID);
            REMOVE.remove(cid_OFFSET_REFS_CID);
            final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
            PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
            REMOVE.remove(cid_OFFSET_CHILDREN_CID);
            final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
            PINNING.unpinCID(cid_OFFSET_NAME_CID);
            REMOVE.remove(cid_OFFSET_NAME_CID);
            DirectTestStruct1.remove(addr + OFFSET_TEST1_STRUCT);
            DirectTestStruct2.remove(addr + OFFSET_TEST2_STRUCT);
            DirectPrimitiveDataTypesStruct.remove(addr + OFFSET_PRIMITIVETYPES_STRUCT);
            DirectStringsStruct.remove(addr + OFFSET_MORESTRINGS2_STRUCT);
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }
    }

    public static int getMiscSimpleChunkLength(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);
    }

    public static long getMiscSimpleChunkID(final long p_id, final int p_index) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), (8 * p_index));
    }

    public static long[] getMiscSimpleChunkIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), 0, len);
    }

    public static long[] getMiscSimpleChunkLocalIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) == 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public static long[] getMiscSimpleChunkRemoteIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public static void setMiscSimpleChunkID(final long p_id, final int p_index, final long p_value) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_MISC_ADDR);
        if ((p_value & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_value & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
            } else {
                RAWWRITE.writeLong(addr2, (8 * p_index), PINNING.translate(p_value) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        }
    }

    public static void setMiscSimpleChunkIDs(final long p_id, final long[] p_misc) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_MISC_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
        }

        if (p_misc == null || p_misc.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_misc.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_MISC_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, p_misc.length);
        for (int i = 0; i < p_misc.length; i ++) {
            if ((p_misc[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_misc[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_misc[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_misc[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_misc[i]);
            }
        }
    }

    public static int getNum(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readInt(addr, OFFSET_NUM);
    }

    public static void setNum(final long p_id, final int p_num) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_NUM, p_num);
    }

    public static long getParentComplexChunkID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_PARENT_ID);
    }

    public static boolean isParentComplexChunkLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_PARENT_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setParentComplexChunkID(final long p_id, final long p_parent_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_parent_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_parent_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, p_parent_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, PINNING.translate(p_parent_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ID, p_parent_id);
        }
    }

    public static double getTemp(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readDouble(addr, OFFSET_TEMP);
    }

    public static void setTemp(final long p_id, final double p_temp) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeDouble(addr, OFFSET_TEMP, p_temp);
    }

    public static long getAnotherSimpleChunkID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_ANOTHER_ID);
    }

    public static boolean isAnotherSimpleChunkLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_ANOTHER_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setAnotherSimpleChunkID(final long p_id, final long p_another_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_another_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_another_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, p_another_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, PINNING.translate(p_another_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ID, p_another_id);
        }
    }

    public static int getRefsLength(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);
    }

    public static short getRefs(final long p_id, final int index) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_REFS_ADDR);
        return RAWREAD.readShort(addr2, (2 * index));
    }

    public static short[] getRefs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readShortArray(RAWREAD.readLong(addr, OFFSET_REFS_ADDR), 0, len);
    }

    public static void setRefs(final long p_id, final int index, final short value) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_REFS_ADDR);
        RAWWRITE.writeShort(addr2, (2 * index), value);
    }

    public static void setRefs(final long p_id, final short[] p_refs) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_REFS_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
        }

        if (p_refs == null || p_refs.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (2 * p_refs.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_REFS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, p_refs.length);
        RAWWRITE.writeShortArray(addr2, 0, p_refs);
    }

    public static int getChildrenComplexChunkLength(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);
    }

    public static long getChildrenComplexChunkID(final long p_id, final int p_index) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public static long[] getChildrenComplexChunkIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public static long[] getChildrenComplexChunkLocalIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) == 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public static long[] getChildrenComplexChunkRemoteIDs(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public static void setChildrenComplexChunkID(final long p_id, final int p_index, final long p_value) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR);
        if ((p_value & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_value & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
            } else {
                RAWWRITE.writeLong(addr2, (8 * p_index), PINNING.translate(p_value) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        }
    }

    public static void setChildrenComplexChunkIDs(final long p_id, final long[] p_children) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
        }

        if (p_children == null || p_children.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_children.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, p_children.length);
        for (int i = 0; i < p_children.length; i ++) {
            if ((p_children[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_children[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_children[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_children[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_children[i]);
            }
        }
    }

    public static String getName(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_NAME_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_NAME_ADDR), 0, len));
    }

    public static void setName(final long p_id, final String p_name) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int len = RAWREAD.readInt(addr, OFFSET_NAME_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_NAME_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_name == null || p_name.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, (p_name == null ? -1 : 0));
            return;
        }

        final byte[] str = p_name.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_NAME_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static long getPersonPersonID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_PERSON_ID);
    }

    public static boolean isPersonPersonLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_PERSON_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setPersonPersonID(final long p_id, final long p_person_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_person_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_person_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, p_person_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, PINNING.translate(p_person_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_PERSON_ID, p_person_id);
        }
    }

    public static Weekday getDay(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int ordinal = RAWREAD.readInt(addr, OFFSET_DAY_ENUM);
        return EnumCache.Weekday_ENUM_VALUES[ordinal];
    }

    public static void setDay(final long p_id, final Weekday p_day) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, p_day.ordinal());
    }

    public static long getCountryCountryID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_COUNTRY_ID);
    }

    public static boolean isCountryCountryLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_COUNTRY_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setCountryCountryID(final long p_id, final long p_country_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_country_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_country_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, p_country_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, PINNING.translate(p_country_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ID, p_country_id);
        }
    }

    public static Month getMonth(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int ordinal = RAWREAD.readInt(addr, OFFSET_MONTH_ENUM);
        return EnumCache.Month_ENUM_VALUES[ordinal];
    }

    public static void setMonth(final long p_id, final Month p_month) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, p_month.ordinal());
    }

    public static long getCityCityID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_CITY_ID);
    }

    public static boolean isCityCityLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_CITY_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setCityCityID(final long p_id, final long p_city_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_city_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_city_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_CITY_ID, p_city_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_CITY_ID, PINNING.translate(p_city_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_CITY_ID, p_city_id);
        }
    }

    public static long getPrimitiveTypes2PrimitiveDataTypesChunkID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_ID);
    }

    public static boolean isPrimitiveTypes2PrimitiveDataTypesChunkLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setPrimitiveTypes2PrimitiveDataTypesChunkID(final long p_id, final long p_primitivetypes2_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_primitivetypes2_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_primitivetypes2_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, p_primitivetypes2_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, PINNING.translate(p_primitivetypes2_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ID, p_primitivetypes2_id);
        }
    }

    public static OS getMyOS(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int ordinal = RAWREAD.readInt(addr, OFFSET_MYOS_ENUM);
        return EnumCache.OS_ENUM_VALUES[ordinal];
    }

    public static void setMyOS(final long p_id, final OS p_myos) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, p_myos.ordinal());
    }

    public static HotDrink getFavoriteDrink(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int ordinal = RAWREAD.readInt(addr, OFFSET_FAVORITEDRINK_ENUM);
        return EnumCache.HotDrink_ENUM_VALUES[ordinal];
    }

    public static void setFavoriteDrink(final long p_id, final HotDrink p_favoritedrink) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, p_favoritedrink.ordinal());
    }

    public static long getMoreStringsStringsChunkID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        return RAWREAD.readLong(addr, OFFSET_MORESTRINGS_ID);
    }

    public static boolean isMoreStringsStringsChunkLocalID(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final long id2 = RAWREAD.readLong(addr, OFFSET_MORESTRINGS_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setMoreStringsStringsChunkID(final long p_id, final long p_morestrings_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        if ((p_morestrings_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_morestrings_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, p_morestrings_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, PINNING.translate(p_morestrings_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ID, p_morestrings_id);
        }
    }

    public static ProgrammingLanguage getFavoriteLang(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        final int ordinal = RAWREAD.readInt(addr, OFFSET_FAVORITELANG_ENUM);
        return EnumCache.ProgrammingLanguage_ENUM_VALUES[ordinal];
    }

    public static void setFavoriteLang(final long p_id, final ProgrammingLanguage p_favoritelang) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, p_favoritelang.ordinal());
    }

    private DirectComplexChunk() {}

    public static DirectComplexChunk use(final long p_id) {
        long addr;

        if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            addr = PINNING.translate(p_id);
        } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
            addr = p_id & 0xFFFFFFFFFFFFL;
        } else {
            throw new RuntimeException("The given CID is not valid or not a local CID!");
        }

        DirectComplexChunk tmp = new DirectComplexChunk();
        tmp.m_addr = addr;
        return tmp;
    }

    public int getMiscSimpleChunkLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);
    }

    public long getMiscSimpleChunkID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), (8 * p_index));
    }

    public long[] getMiscSimpleChunkIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), 0, len);
    }

    public long[] getMiscSimpleChunkLocalIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) == 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public long[] getMiscSimpleChunkRemoteIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public void setMiscSimpleChunkID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR);
        if ((p_value & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_value & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
            } else {
                RAWWRITE.writeLong(addr2, (8 * p_index), PINNING.translate(p_value) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        }
    }

    public void setMiscSimpleChunkIDs(final long[] p_misc) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_MISC_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_MISC_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_MISC_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_MISC_LENGTH, 0);
        }

        if (p_misc == null || p_misc.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_misc.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_MISC_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_MISC_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_MISC_LENGTH, p_misc.length);
        for (int i = 0; i < p_misc.length; i ++) {
            if ((p_misc[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_misc[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_misc[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_misc[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_misc[i]);
            }
        }
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

    public long getParentComplexChunkID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PARENT_ID);
    }

    public void setParentComplexChunkID(final long p_parent_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_parent_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_parent_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_PARENT_ID, p_parent_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_PARENT_ID, PINNING.translate(p_parent_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_PARENT_ID, p_parent_id);
        }
    }

    public double getTemp() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readDouble(m_addr, OFFSET_TEMP);
    }

    public void setTemp(final double p_temp) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeDouble(m_addr, OFFSET_TEMP, p_temp);
    }

    public long getAnotherSimpleChunkID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_ANOTHER_ID);
    }

    public void setAnotherSimpleChunkID(final long p_another_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_another_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_another_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_ANOTHER_ID, p_another_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_ANOTHER_ID, PINNING.translate(p_another_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_ANOTHER_ID, p_another_id);
        }
    }

    public int getRefsLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_REFS_LENGTH);
    }

    public short getRefs(final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_REFS_ADDR);
        return RAWREAD.readShort(addr, (2 * index));
    }

    public short[] getRefs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_REFS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readShortArray(RAWREAD.readLong(m_addr, OFFSET_REFS_ADDR), 0, len);
    }

    public void setRefs(final int index, final short value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_REFS_ADDR);
        RAWWRITE.writeShort(addr, (2 * index), value);
    }

    public void setRefs(final short[] p_refs) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_REFS_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_REFS_CID);

        if (cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(m_addr, OFFSET_REFS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_REFS_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_REFS_LENGTH, 0);
        }

        if (p_refs == null || p_refs.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (2 * p_refs.length));
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_REFS_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_REFS_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_REFS_LENGTH, p_refs.length);
        RAWWRITE.writeShortArray(addr, 0, p_refs);
    }

    public int getChildrenComplexChunkLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);
    }

    public long getChildrenComplexChunkID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public long[] getChildrenComplexChunkIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public long[] getChildrenComplexChunkLocalIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) == 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public long[] getChildrenComplexChunkRemoteIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), 0, len);
        final long[] tmp = new long[ids.length];
        int j = 0;
        for (int i = 0; i < ids.length; i ++) {
            if ((ids[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                tmp[j++] = ids[i];
            }
        }
        final long[] result = new long[j];
        System.arraycopy(tmp, 0, result, 0, j);
        return result;
    }

    public void setChildrenComplexChunkID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR);
        if ((p_value & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_value & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
            } else {
                RAWWRITE.writeLong(addr2, (8 * p_index), PINNING.translate(p_value) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        }
    }

    public void setChildrenComplexChunkIDs(final long[] p_children) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_CHILDREN_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_CHILDREN_LENGTH, 0);
        }

        if (p_children == null || p_children.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_children.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_CHILDREN_LENGTH, p_children.length);
        for (int i = 0; i < p_children.length; i ++) {
            if ((p_children[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_children[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_children[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_children[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_children[i]);
            }
        }
    }

    public String getName() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NAME_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(m_addr, OFFSET_NAME_ADDR), 0, len));
    }

    public void setName(final String p_name) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NAME_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_NAME_CID);

        if (cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_name == null || p_name.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_NAME_LENGTH, (p_name == null ? -1 : 0));
            return;
        }

        final byte[] str = p_name.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_NAME_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_NAME_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_NAME_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr, 0, str);
    }

    public long getPersonPersonID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PERSON_ID);
    }

    public void setPersonPersonID(final long p_person_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_person_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_person_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_PERSON_ID, p_person_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_PERSON_ID, PINNING.translate(p_person_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_PERSON_ID, p_person_id);
        }
    }

    public Weekday getDay() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(m_addr, OFFSET_DAY_ENUM);
        return EnumCache.Weekday_ENUM_VALUES[ordinal];
    }

    public void setDay(final Weekday p_day) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_DAY_ENUM, p_day.ordinal());
    }

    public long getCountryCountryID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_COUNTRY_ID);
    }

    public void setCountryCountryID(final long p_country_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_country_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_country_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_COUNTRY_ID, p_country_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_COUNTRY_ID, PINNING.translate(p_country_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_COUNTRY_ID, p_country_id);
        }
    }

    public Month getMonth() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(m_addr, OFFSET_MONTH_ENUM);
        return EnumCache.Month_ENUM_VALUES[ordinal];
    }

    public void setMonth(final Month p_month) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_MONTH_ENUM, p_month.ordinal());
    }

    public long getCityCityID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_CITY_ID);
    }

    public void setCityCityID(final long p_city_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_city_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_city_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_CITY_ID, p_city_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_CITY_ID, PINNING.translate(p_city_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_CITY_ID, p_city_id);
        }
    }

    public long getPrimitiveTypes2PrimitiveDataTypesChunkID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PRIMITIVETYPES2_ID);
    }

    public void setPrimitiveTypes2PrimitiveDataTypesChunkID(final long p_primitivetypes2_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_primitivetypes2_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_primitivetypes2_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_PRIMITIVETYPES2_ID, p_primitivetypes2_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_PRIMITIVETYPES2_ID, PINNING.translate(p_primitivetypes2_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_PRIMITIVETYPES2_ID, p_primitivetypes2_id);
        }
    }

    public OS getMyOS() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(m_addr, OFFSET_MYOS_ENUM);
        return EnumCache.OS_ENUM_VALUES[ordinal];
    }

    public void setMyOS(final OS p_myos) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_MYOS_ENUM, p_myos.ordinal());
    }

    public HotDrink getFavoriteDrink() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(m_addr, OFFSET_FAVORITEDRINK_ENUM);
        return EnumCache.HotDrink_ENUM_VALUES[ordinal];
    }

    public void setFavoriteDrink(final HotDrink p_favoritedrink) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_FAVORITEDRINK_ENUM, p_favoritedrink.ordinal());
    }

    public long getMoreStringsStringsChunkID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_MORESTRINGS_ID);
    }

    public void setMoreStringsStringsChunkID(final long p_morestrings_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_morestrings_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_morestrings_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_MORESTRINGS_ID, p_morestrings_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_MORESTRINGS_ID, PINNING.translate(p_morestrings_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_MORESTRINGS_ID, p_morestrings_id);
        }
    }

    public ProgrammingLanguage getFavoriteLang() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(m_addr, OFFSET_FAVORITELANG_ENUM);
        return EnumCache.ProgrammingLanguage_ENUM_VALUES[ordinal];
    }

    public void setFavoriteLang(final ProgrammingLanguage p_favoritelang) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_FAVORITELANG_ENUM, p_favoritelang.ordinal());
    }

    test1 test1() {
        return new test1(m_addr + OFFSET_TEST1_STRUCT);
    }

    test2 test2() {
        return new test2(m_addr + OFFSET_TEST2_STRUCT);
    }

    primitiveTypes primitiveTypes() {
        return new primitiveTypes(m_addr + OFFSET_PRIMITIVETYPES_STRUCT);
    }

    moreStrings2 moreStrings2() {
        return new moreStrings2(m_addr + OFFSET_MORESTRINGS2_STRUCT);
    }

    @Override
    public void close() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        m_addr = 0x0L;
    }

    public static final class test1 {

        private final long m_addr;

        private test1(final long p_addr) {
            m_addr = p_addr;
        }

        public boolean getB() {
            return DirectTestStruct1.getB(m_addr);
        }

        public void setB(final boolean p_b) {
            DirectTestStruct1.setB(m_addr, p_b);
        }

        public static boolean getB(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getB(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setB(final long p_id, final boolean p_b) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setB(addr + OFFSET_TEST1_STRUCT, p_b);
        }

        public int getCLength() {
            return DirectTestStruct1.getCLength(m_addr);
        }

        public static int getCLength(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getCLength(addr + OFFSET_TEST1_STRUCT);
        }

        public char getC(final int p_index) {
            return DirectTestStruct1.getC(m_addr, p_index);
        }

        public static char getC(final long p_id, final int p_index) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getC(addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public char[] getC() {
            return DirectTestStruct1.getC(m_addr);
        }

        public static char[] getC(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getC(addr + OFFSET_TEST1_STRUCT);
        }

        public void setC(final int p_index, final char p_value) {
            DirectTestStruct1.setC(m_addr, p_index, p_value);
        }

        public static void setC(final long p_id, final int p_index, final char p_value) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setC(addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public void setC(final char[] p_c) {
            DirectTestStruct1.setC(m_addr, p_c);
        }

        public static void setC(final long p_id, final char[] p_c) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setC(addr + OFFSET_TEST1_STRUCT, p_c);
        }

        public String getS() {
            return DirectTestStruct1.getS(m_addr);
        }

        public void setS(final String p_s) {
            DirectTestStruct1.setS(m_addr, p_s);
        }

        public static String getS(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getS(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setS(final long p_id, final String p_s) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setS(addr + OFFSET_TEST1_STRUCT, p_s);
        }

        public int getI() {
            return DirectTestStruct1.getI(m_addr);
        }

        public void setI(final int p_i) {
            DirectTestStruct1.setI(m_addr, p_i);
        }

        public static int getI(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getI(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setI(final long p_id, final int p_i) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setI(addr + OFFSET_TEST1_STRUCT, p_i);
        }

        public int getDLength() {
            return DirectTestStruct1.getDLength(m_addr);
        }

        public static int getDLength(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getDLength(addr + OFFSET_TEST1_STRUCT);
        }

        public double getD(final int p_index) {
            return DirectTestStruct1.getD(m_addr, p_index);
        }

        public static double getD(final long p_id, final int p_index) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getD(addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public double[] getD() {
            return DirectTestStruct1.getD(m_addr);
        }

        public static double[] getD(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct1.getD(addr + OFFSET_TEST1_STRUCT);
        }

        public void setD(final int p_index, final double p_value) {
            DirectTestStruct1.setD(m_addr, p_index, p_value);
        }

        public static void setD(final long p_id, final int p_index, final double p_value) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setD(addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public void setD(final double[] p_d) {
            DirectTestStruct1.setD(m_addr, p_d);
        }

        public static void setD(final long p_id, final double[] p_d) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct1.setD(addr + OFFSET_TEST1_STRUCT, p_d);
        }
    }

    public static final class test2 {

        private final long m_addr;

        private test2(final long p_addr) {
            m_addr = p_addr;
        }

        public String getS() {
            return DirectTestStruct2.getS(m_addr);
        }

        public void setS(final String p_s) {
            DirectTestStruct2.setS(m_addr, p_s);
        }

        public static String getS(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getS(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setS(final long p_id, final String p_s) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setS(addr + OFFSET_TEST2_STRUCT, p_s);
        }

        public int getNumLength() {
            return DirectTestStruct2.getNumLength(m_addr);
        }

        public static int getNumLength(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getNumLength(addr + OFFSET_TEST2_STRUCT);
        }

        public int getNum(final int p_index) {
            return DirectTestStruct2.getNum(m_addr, p_index);
        }

        public static int getNum(final long p_id, final int p_index) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getNum(addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public int[] getNum() {
            return DirectTestStruct2.getNum(m_addr);
        }

        public static int[] getNum(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getNum(addr + OFFSET_TEST2_STRUCT);
        }

        public void setNum(final int p_index, final int p_value) {
            DirectTestStruct2.setNum(m_addr, p_index, p_value);
        }

        public static void setNum(final long p_id, final int p_index, final int p_value) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setNum(addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public void setNum(final int[] p_num) {
            DirectTestStruct2.setNum(m_addr, p_num);
        }

        public static void setNum(final long p_id, final int[] p_num) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setNum(addr + OFFSET_TEST2_STRUCT, p_num);
        }

        public short getX1() {
            return DirectTestStruct2.getX1(m_addr);
        }

        public void setX1(final short p_x1) {
            DirectTestStruct2.setX1(m_addr, p_x1);
        }

        public static short getX1(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getX1(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX1(final long p_id, final short p_x1) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setX1(addr + OFFSET_TEST2_STRUCT, p_x1);
        }

        public short getX2() {
            return DirectTestStruct2.getX2(m_addr);
        }

        public void setX2(final short p_x2) {
            DirectTestStruct2.setX2(m_addr, p_x2);
        }

        public static short getX2(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getX2(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX2(final long p_id, final short p_x2) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setX2(addr + OFFSET_TEST2_STRUCT, p_x2);
        }

        public int getAlphaLength() {
            return DirectTestStruct2.getAlphaLength(m_addr);
        }

        public static int getAlphaLength(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getAlphaLength(addr + OFFSET_TEST2_STRUCT);
        }

        public char getAlpha(final int p_index) {
            return DirectTestStruct2.getAlpha(m_addr, p_index);
        }

        public static char getAlpha(final long p_id, final int p_index) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getAlpha(addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public char[] getAlpha() {
            return DirectTestStruct2.getAlpha(m_addr);
        }

        public static char[] getAlpha(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getAlpha(addr + OFFSET_TEST2_STRUCT);
        }

        public void setAlpha(final int p_index, final char p_value) {
            DirectTestStruct2.setAlpha(m_addr, p_index, p_value);
        }

        public static void setAlpha(final long p_id, final int p_index, final char p_value) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setAlpha(addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public void setAlpha(final char[] p_alpha) {
            DirectTestStruct2.setAlpha(m_addr, p_alpha);
        }

        public static void setAlpha(final long p_id, final char[] p_alpha) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setAlpha(addr + OFFSET_TEST2_STRUCT, p_alpha);
        }

        public short getY1() {
            return DirectTestStruct2.getY1(m_addr);
        }

        public void setY1(final short p_y1) {
            DirectTestStruct2.setY1(m_addr, p_y1);
        }

        public static short getY1(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getY1(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY1(final long p_id, final short p_y1) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setY1(addr + OFFSET_TEST2_STRUCT, p_y1);
        }

        public short getY2() {
            return DirectTestStruct2.getY2(m_addr);
        }

        public void setY2(final short p_y2) {
            DirectTestStruct2.setY2(m_addr, p_y2);
        }

        public static short getY2(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectTestStruct2.getY2(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY2(final long p_id, final short p_y2) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectTestStruct2.setY2(addr + OFFSET_TEST2_STRUCT, p_y2);
        }
    }

    public static final class primitiveTypes {

        private final long m_addr;

        private primitiveTypes(final long p_addr) {
            m_addr = p_addr;
        }

        public boolean getB() {
            return DirectPrimitiveDataTypesStruct.getB(m_addr);
        }

        public void setB(final boolean p_b) {
            DirectPrimitiveDataTypesStruct.setB(m_addr, p_b);
        }

        public static boolean getB(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getB(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setB(final long p_id, final boolean p_b) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setB(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b);
        }

        public byte getB2() {
            return DirectPrimitiveDataTypesStruct.getB2(m_addr);
        }

        public void setB2(final byte p_b2) {
            DirectPrimitiveDataTypesStruct.setB2(m_addr, p_b2);
        }

        public static byte getB2(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getB2(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setB2(final long p_id, final byte p_b2) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setB2(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b2);
        }

        public char getC() {
            return DirectPrimitiveDataTypesStruct.getC(m_addr);
        }

        public void setC(final char p_c) {
            DirectPrimitiveDataTypesStruct.setC(m_addr, p_c);
        }

        public static char getC(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getC(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setC(final long p_id, final char p_c) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setC(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_c);
        }

        public double getD() {
            return DirectPrimitiveDataTypesStruct.getD(m_addr);
        }

        public void setD(final double p_d) {
            DirectPrimitiveDataTypesStruct.setD(m_addr, p_d);
        }

        public static double getD(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getD(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setD(final long p_id, final double p_d) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setD(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_d);
        }

        public float getF() {
            return DirectPrimitiveDataTypesStruct.getF(m_addr);
        }

        public void setF(final float p_f) {
            DirectPrimitiveDataTypesStruct.setF(m_addr, p_f);
        }

        public static float getF(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getF(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setF(final long p_id, final float p_f) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setF(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_f);
        }

        public int getNum() {
            return DirectPrimitiveDataTypesStruct.getNum(m_addr);
        }

        public void setNum(final int p_num) {
            DirectPrimitiveDataTypesStruct.setNum(m_addr, p_num);
        }

        public static int getNum(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getNum(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setNum(final long p_id, final int p_num) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setNum(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_num);
        }

        public long getBignum() {
            return DirectPrimitiveDataTypesStruct.getBignum(m_addr);
        }

        public void setBignum(final long p_bignum) {
            DirectPrimitiveDataTypesStruct.setBignum(m_addr, p_bignum);
        }

        public static long getBignum(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getBignum(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setBignum(final long p_id, final long p_bignum) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setBignum(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_bignum);
        }

        public short getS() {
            return DirectPrimitiveDataTypesStruct.getS(m_addr);
        }

        public void setS(final short p_s) {
            DirectPrimitiveDataTypesStruct.setS(m_addr, p_s);
        }

        public static short getS(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectPrimitiveDataTypesStruct.getS(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setS(final long p_id, final short p_s) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectPrimitiveDataTypesStruct.setS(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_s);
        }
    }

    public static final class moreStrings2 {

        private final long m_addr;

        private moreStrings2(final long p_addr) {
            m_addr = p_addr;
        }

        public String getS1() {
            return DirectStringsStruct.getS1(m_addr);
        }

        public void setS1(final String p_s1) {
            DirectStringsStruct.setS1(m_addr, p_s1);
        }

        public static String getS1(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectStringsStruct.getS1(addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS1(final long p_id, final String p_s1) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectStringsStruct.setS1(addr + OFFSET_MORESTRINGS2_STRUCT, p_s1);
        }

        public String getS2() {
            return DirectStringsStruct.getS2(m_addr);
        }

        public void setS2(final String p_s2) {
            DirectStringsStruct.setS2(m_addr, p_s2);
        }

        public static String getS2(final long p_id) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            return DirectStringsStruct.getS2(addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS2(final long p_id, final String p_s2) {
            long addr;

            if ((p_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    throw new RuntimeException("The given CID is not valid or not a local CID!");
                }

                addr = PINNING.translate(p_id);
            } else if (p_id != 0xFFFFFFFFFFFFFFFFL) {
                addr = p_id & 0xFFFFFFFFFFFFL;
            } else {
                throw new RuntimeException("The given CID is not valid or not a local CID!");
            }

            DirectStringsStruct.setS2(addr + OFFSET_MORESTRINGS2_STRUCT, p_s2);
        }
    }
}
