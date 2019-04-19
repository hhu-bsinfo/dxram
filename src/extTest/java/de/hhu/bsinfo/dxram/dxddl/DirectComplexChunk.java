package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectComplexChunk implements AutoCloseable {

    private static final int OFFSET_MISC_LENGTH = 0;
    private static final int OFFSET_MISC_CID = 4;
    private static final int OFFSET_MISC_ADDR = 12;
    private static final int OFFSET_NUM = 20;
    private static final int OFFSET_PARENT_CID = 24;
    private static final int OFFSET_PARENT_ADDR = 32;
    private static final int OFFSET_TEMP = 40;
    private static final int OFFSET_ANOTHER_CID = 48;
    private static final int OFFSET_ANOTHER_ADDR = 56;
    private static final int OFFSET_REFS_LENGTH = 64;
    private static final int OFFSET_REFS_CID = 68;
    private static final int OFFSET_REFS_ADDR = 76;
    private static final int OFFSET_TEST1_STRUCT = 84;
    private static final int OFFSET_CHILDREN_LENGTH = 149;
    private static final int OFFSET_CHILDREN_CID = 153;
    private static final int OFFSET_CHILDREN_ADDR = 161;
    private static final int OFFSET_NAME_LENGTH = 169;
    private static final int OFFSET_NAME_CID = 173;
    private static final int OFFSET_NAME_ADDR = 181;
    private static final int OFFSET_PERSON_CID = 189;
    private static final int OFFSET_PERSON_ADDR = 197;
    private static final int OFFSET_DAY_ENUM = 205;
    private static final int OFFSET_TEST2_STRUCT = 209;
    private static final int OFFSET_COUNTRY_CID = 277;
    private static final int OFFSET_COUNTRY_ADDR = 285;
    private static final int OFFSET_MONTH_ENUM = 293;
    private static final int OFFSET_CITY_CID = 297;
    private static final int OFFSET_CITY_ADDR = 305;
    private static final int OFFSET_PRIMITIVETYPES_STRUCT = 313;
    private static final int OFFSET_PRIMITIVETYPES2_CID = 343;
    private static final int OFFSET_PRIMITIVETYPES2_ADDR = 351;
    private static final int OFFSET_MYOS_ENUM = 359;
    private static final int OFFSET_FAVORITEDRINK_ENUM = 363;
    private static final int OFFSET_MORESTRINGS_CID = 367;
    private static final int OFFSET_MORESTRINGS_ADDR = 375;
    private static final int OFFSET_FAVORITELANG_ENUM = 383;
    private static final int OFFSET_MORESTRINGS2_STRUCT = 387;
    private static final int SIZE = 427;
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
            DirectTestStruct1.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectTestStruct2.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectPrimitiveDataTypesStruct.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
            DirectStringsStruct.init(create, create_reserved, reserve, remove, pinning, rawread, rawwrite);
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
        RAWWRITE.writeLong(addr, OFFSET_MISC_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_ANOTHER_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_REFS_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_PERSON_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_COUNTRY_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_CITY_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_CID, -1);
        RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_PERSON_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_CITY_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ADDR, 0);
        RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, 0);
        DirectTestStruct1.create(addr + OFFSET_TEST1_STRUCT);
        DirectTestStruct2.create(addr + OFFSET_TEST2_STRUCT);
        DirectPrimitiveDataTypesStruct.create(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        DirectStringsStruct.create(addr + OFFSET_MORESTRINGS2_STRUCT);
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
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CITY_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CITY_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CITY_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_REFS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PERSON_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CITY_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ADDR, 0);
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

    public static void remove(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long cid_OFFSET_MISC_CID = RAWREAD.readLong(addr, OFFSET_MISC_CID);
        PINNING.unpinCID(cid_OFFSET_MISC_CID);
        REMOVE.remove(cid_OFFSET_MISC_CID);
        final long cid_OFFSET_PARENT_CID = RAWREAD.readLong(addr, OFFSET_PARENT_CID);
        PINNING.unpinCID(cid_OFFSET_PARENT_CID);
        REMOVE.remove(cid_OFFSET_PARENT_CID);
        final long cid_OFFSET_ANOTHER_CID = RAWREAD.readLong(addr, OFFSET_ANOTHER_CID);
        PINNING.unpinCID(cid_OFFSET_ANOTHER_CID);
        REMOVE.remove(cid_OFFSET_ANOTHER_CID);
        final long cid_OFFSET_REFS_CID = RAWREAD.readLong(addr, OFFSET_REFS_CID);
        PINNING.unpinCID(cid_OFFSET_REFS_CID);
        REMOVE.remove(cid_OFFSET_REFS_CID);
        final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
        PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
        REMOVE.remove(cid_OFFSET_CHILDREN_CID);
        final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
        PINNING.unpinCID(cid_OFFSET_NAME_CID);
        REMOVE.remove(cid_OFFSET_NAME_CID);
        final long cid_OFFSET_PERSON_CID = RAWREAD.readLong(addr, OFFSET_PERSON_CID);
        PINNING.unpinCID(cid_OFFSET_PERSON_CID);
        REMOVE.remove(cid_OFFSET_PERSON_CID);
        final long cid_OFFSET_COUNTRY_CID = RAWREAD.readLong(addr, OFFSET_COUNTRY_CID);
        PINNING.unpinCID(cid_OFFSET_COUNTRY_CID);
        REMOVE.remove(cid_OFFSET_COUNTRY_CID);
        final long cid_OFFSET_CITY_CID = RAWREAD.readLong(addr, OFFSET_CITY_CID);
        PINNING.unpinCID(cid_OFFSET_CITY_CID);
        REMOVE.remove(cid_OFFSET_CITY_CID);
        final long cid_OFFSET_PRIMITIVETYPES2_CID = RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_CID);
        PINNING.unpinCID(cid_OFFSET_PRIMITIVETYPES2_CID);
        REMOVE.remove(cid_OFFSET_PRIMITIVETYPES2_CID);
        final long cid_OFFSET_MORESTRINGS_CID = RAWREAD.readLong(addr, OFFSET_MORESTRINGS_CID);
        PINNING.unpinCID(cid_OFFSET_MORESTRINGS_CID);
        REMOVE.remove(cid_OFFSET_MORESTRINGS_CID);
        DirectTestStruct1.remove(addr + OFFSET_TEST1_STRUCT);
        DirectTestStruct2.remove(addr + OFFSET_TEST2_STRUCT);
        DirectPrimitiveDataTypesStruct.remove(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        DirectStringsStruct.remove(addr + OFFSET_MORESTRINGS2_STRUCT);
        PINNING.unpinCID(p_cid);
        REMOVE.remove(p_cid);
    }

    public static void remove(final long[] p_cids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        for (int i = 0; i < p_cids.length; i ++) {
            final long addr = PINNING.translate(p_cids[i]);
            final long cid_OFFSET_MISC_CID = RAWREAD.readLong(addr, OFFSET_MISC_CID);
            PINNING.unpinCID(cid_OFFSET_MISC_CID);
            REMOVE.remove(cid_OFFSET_MISC_CID);
            final long cid_OFFSET_PARENT_CID = RAWREAD.readLong(addr, OFFSET_PARENT_CID);
            PINNING.unpinCID(cid_OFFSET_PARENT_CID);
            REMOVE.remove(cid_OFFSET_PARENT_CID);
            final long cid_OFFSET_ANOTHER_CID = RAWREAD.readLong(addr, OFFSET_ANOTHER_CID);
            PINNING.unpinCID(cid_OFFSET_ANOTHER_CID);
            REMOVE.remove(cid_OFFSET_ANOTHER_CID);
            final long cid_OFFSET_REFS_CID = RAWREAD.readLong(addr, OFFSET_REFS_CID);
            PINNING.unpinCID(cid_OFFSET_REFS_CID);
            REMOVE.remove(cid_OFFSET_REFS_CID);
            final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
            PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
            REMOVE.remove(cid_OFFSET_CHILDREN_CID);
            final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
            PINNING.unpinCID(cid_OFFSET_NAME_CID);
            REMOVE.remove(cid_OFFSET_NAME_CID);
            final long cid_OFFSET_PERSON_CID = RAWREAD.readLong(addr, OFFSET_PERSON_CID);
            PINNING.unpinCID(cid_OFFSET_PERSON_CID);
            REMOVE.remove(cid_OFFSET_PERSON_CID);
            final long cid_OFFSET_COUNTRY_CID = RAWREAD.readLong(addr, OFFSET_COUNTRY_CID);
            PINNING.unpinCID(cid_OFFSET_COUNTRY_CID);
            REMOVE.remove(cid_OFFSET_COUNTRY_CID);
            final long cid_OFFSET_CITY_CID = RAWREAD.readLong(addr, OFFSET_CITY_CID);
            PINNING.unpinCID(cid_OFFSET_CITY_CID);
            REMOVE.remove(cid_OFFSET_CITY_CID);
            final long cid_OFFSET_PRIMITIVETYPES2_CID = RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_CID);
            PINNING.unpinCID(cid_OFFSET_PRIMITIVETYPES2_CID);
            REMOVE.remove(cid_OFFSET_PRIMITIVETYPES2_CID);
            final long cid_OFFSET_MORESTRINGS_CID = RAWREAD.readLong(addr, OFFSET_MORESTRINGS_CID);
            PINNING.unpinCID(cid_OFFSET_MORESTRINGS_CID);
            REMOVE.remove(cid_OFFSET_MORESTRINGS_CID);
            DirectTestStruct1.remove(addr + OFFSET_TEST1_STRUCT);
            DirectTestStruct2.remove(addr + OFFSET_TEST2_STRUCT);
            DirectPrimitiveDataTypesStruct.remove(addr + OFFSET_PRIMITIVETYPES_STRUCT);
            DirectStringsStruct.remove(addr + OFFSET_MORESTRINGS2_STRUCT);
            PINNING.unpinCID(p_cids[i]);
            REMOVE.remove(p_cids[i]);
        }
    }

    public static int getMiscSimpleChunkLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);
    }

    public static long getMiscSimpleChunkCID(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), (8 * p_index));
    }

    public static long getMiscSimpleChunkAddress(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), (8 * (len + p_index)));
    }

    public static long[] getMiscSimpleChunkCIDs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), 0, len);
    }

    public static long[] getMiscSimpleChunkAddresses(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_MISC_ADDR), (8 * len), len);
    }

    public static int getMiscSimpleChunkLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);
    }

    public static long getMiscSimpleChunkCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_MISC_ADDR), (8 * p_index));
    }

    public static long getMiscSimpleChunkAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_MISC_ADDR), (8 * (len + p_index)));
    }

    public static long[] getMiscSimpleChunkCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_MISC_ADDR), 0, len);
    }

    public static long[] getMiscSimpleChunkAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_MISC_ADDR), (8 * len), len);
    }

    public static void setMiscSimpleChunkCID(final long p_cid, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_MISC_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setMiscSimpleChunkCIDs(final long p_cid, final long[] p_misc) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_MISC_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_MISC_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_MISC_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, 0);
        }

        if (p_misc == null || p_misc.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_misc.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_MISC_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_MISC_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_MISC_LENGTH, p_misc.length);
        RAWWRITE.writeLongArray(addr2, 0, p_misc);
        for (int i = 0; i < p_misc.length; i ++) {
            final long addr3 = PINNING.translate(p_misc[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_misc.length + i)), addr3);
        }
    }

    public static void setMiscSimpleChunkCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_MISC_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setMiscSimpleChunkCIDsViaAddress(final long p_addr, final long[] p_misc) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_MISC_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_MISC_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_MISC_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_MISC_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_MISC_LENGTH, 0);
        }

        if (p_misc == null || p_misc.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_misc.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_MISC_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_MISC_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_MISC_LENGTH, p_misc.length);
        RAWWRITE.writeLongArray(addr2, 0, p_misc);
        for (int i = 0; i < p_misc.length; i ++) {
            final long addr3 = PINNING.translate(p_misc[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_misc.length + i)), addr3);
        }
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

    public static long getParentComplexChunkCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PARENT_CID);
    }

    public static long getParentComplexChunkAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PARENT_ADDR);
    }

    public static long getParentComplexChunkCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PARENT_CID);
    }

    public static long getParentComplexChunkAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PARENT_ADDR);
    }

    public static void setParentComplexChunkCID(final long p_cid, final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, addr2);
    }

    public static void setParentComplexChunkCIDViaAddress(final long p_addr, final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PARENT_ADDR, addr2);
    }

    public static double getTemp(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readDouble(addr, OFFSET_TEMP);
    }

    public static double getTempViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readDouble(p_addr, OFFSET_TEMP);
    }

    public static void setTemp(final long p_cid, final double p_temp) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeDouble(addr, OFFSET_TEMP, p_temp);
    }

    public static void setTempViaAddress(final long p_addr, final double p_temp) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeDouble(p_addr, OFFSET_TEMP, p_temp);
    }

    public static long getAnotherSimpleChunkCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_ANOTHER_CID);
    }

    public static long getAnotherSimpleChunkAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_ANOTHER_ADDR);
    }

    public static long getAnotherSimpleChunkCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_ANOTHER_CID);
    }

    public static long getAnotherSimpleChunkAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_ANOTHER_ADDR);
    }

    public static void setAnotherSimpleChunkCID(final long p_cid, final long p_another_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_another_cid);
        RAWWRITE.writeLong(addr, OFFSET_ANOTHER_CID, p_another_cid);
        RAWWRITE.writeLong(addr, OFFSET_ANOTHER_ADDR, addr2);
    }

    public static void setAnotherSimpleChunkCIDViaAddress(final long p_addr, final long p_another_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_another_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_ANOTHER_CID, p_another_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_ANOTHER_ADDR, addr2);
    }

    public static int getRefsLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);
    }

    public static int getRefsLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_REFS_LENGTH);
    }

    public static short getRefs(final long p_cid, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_REFS_ADDR);
        return RAWREAD.readShort(addr2, (2 * index));
    }

    public static short getRefsViaAddress(final long p_addr, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_REFS_ADDR);
        return RAWREAD.readShort(addr2, (2 * index));
    }

    public static short[] getRefs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readShortArray(RAWREAD.readLong(addr, OFFSET_REFS_ADDR), 0, len);
    }

    public static short[] getRefsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_REFS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readShortArray(RAWREAD.readLong(p_addr, OFFSET_REFS_ADDR), 0, len);
    }

    public static void setRefs(final long p_cid, final int index, final short value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_REFS_ADDR);
        RAWWRITE.writeShort(addr2, (2 * index), value);
    }

    public static void setRefsViaAddress(final long p_addr, final int index, final short value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_REFS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_REFS_ADDR);
        RAWWRITE.writeShort(addr2, (2 * index), value);
    }

    public static void setRefs(final long p_cid, final short[] p_refs) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_REFS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_REFS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_REFS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_REFS_ADDR, 0);
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

    public static void setRefsViaAddress(final long p_addr, final short[] p_refs) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_REFS_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_REFS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_REFS_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_REFS_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_REFS_LENGTH, 0);
        }

        if (p_refs == null || p_refs.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (2 * p_refs.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_REFS_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_REFS_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_REFS_LENGTH, p_refs.length);
        RAWWRITE.writeShortArray(addr2, 0, p_refs);
    }

    public static int getChildrenComplexChunkLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);
    }

    public static long getChildrenComplexChunkCID(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public static long getChildrenComplexChunkAddress(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), (8 * (len + p_index)));
    }

    public static long[] getChildrenComplexChunkCIDs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public static long[] getChildrenComplexChunkAddresses(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR), (8 * len), len);
    }

    public static int getChildrenComplexChunkLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);
    }

    public static long getChildrenComplexChunkCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public static long getChildrenComplexChunkAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * (len + p_index)));
    }

    public static long[] getChildrenComplexChunkCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public static long[] getChildrenComplexChunkAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * len), len);
    }

    public static void setChildrenComplexChunkCID(final long p_cid, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_CHILDREN_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setChildrenComplexChunkCIDs(final long p_cid, final long[] p_children) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
        }

        if (p_children == null || p_children.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_children.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, p_children.length);
        RAWWRITE.writeLongArray(addr2, 0, p_children);
        for (int i = 0; i < p_children.length; i ++) {
            final long addr3 = PINNING.translate(p_children[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_children.length + i)), addr3);
        }
    }

    public static void setChildrenComplexChunkCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setChildrenComplexChunkCIDsViaAddress(final long p_addr, final long[] p_children) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_CHILDREN_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_CHILDREN_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_CHILDREN_LENGTH, 0);
        }

        if (p_children == null || p_children.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_children.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_CHILDREN_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_CHILDREN_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_CHILDREN_LENGTH, p_children.length);
        RAWWRITE.writeLongArray(addr2, 0, p_children);
        for (int i = 0; i < p_children.length; i ++) {
            final long addr3 = PINNING.translate(p_children[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_children.length + i)), addr3);
        }
    }

    public static String getName(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NAME_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_NAME_ADDR), 0, len));
    }

    public static String getNameViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NAME_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_NAME_ADDR), 0, len));
    }

    public static void setName(final long p_cid, final String p_name) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NAME_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_NAME_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_name == null || p_name.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
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

    public static void setNameViaAddress(final long p_addr, final String p_name) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NAME_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_NAME_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_NAME_LENGTH, (p_name == null ? -1 : 0));
        }

        if (p_name == null || p_name.length() == 0) {
            return;
        }

        final byte[] str = p_name.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_NAME_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_NAME_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_NAME_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static long getPersonPersonCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PERSON_CID);
    }

    public static long getPersonPersonAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PERSON_ADDR);
    }

    public static long getPersonPersonCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PERSON_CID);
    }

    public static long getPersonPersonAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PERSON_ADDR);
    }

    public static void setPersonPersonCID(final long p_cid, final long p_person_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_person_cid);
        RAWWRITE.writeLong(addr, OFFSET_PERSON_CID, p_person_cid);
        RAWWRITE.writeLong(addr, OFFSET_PERSON_ADDR, addr2);
    }

    public static void setPersonPersonCIDViaAddress(final long p_addr, final long p_person_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_person_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PERSON_CID, p_person_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PERSON_ADDR, addr2);
    }

    public static Weekday getDay(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int ordinal = RAWREAD.readInt(addr, OFFSET_DAY_ENUM);
        return EnumCache.Weekday_ENUM_VALUES[ordinal];
    }

    public static Weekday getDayViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(p_addr, OFFSET_DAY_ENUM);
        return EnumCache.Weekday_ENUM_VALUES[ordinal];
    }

    public static void setDay(final long p_cid, final Weekday p_day) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_DAY_ENUM, p_day.ordinal());
    }

    public static void setDayViaAddress(final long p_addr, final Weekday p_day) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_DAY_ENUM, p_day.ordinal());
    }

    public static long getCountryCountryCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_COUNTRY_CID);
    }

    public static long getCountryCountryAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_COUNTRY_ADDR);
    }

    public static long getCountryCountryCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_COUNTRY_CID);
    }

    public static long getCountryCountryAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_COUNTRY_ADDR);
    }

    public static void setCountryCountryCID(final long p_cid, final long p_country_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_country_cid);
        RAWWRITE.writeLong(addr, OFFSET_COUNTRY_CID, p_country_cid);
        RAWWRITE.writeLong(addr, OFFSET_COUNTRY_ADDR, addr2);
    }

    public static void setCountryCountryCIDViaAddress(final long p_addr, final long p_country_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_country_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_COUNTRY_CID, p_country_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_COUNTRY_ADDR, addr2);
    }

    public static Month getMonth(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int ordinal = RAWREAD.readInt(addr, OFFSET_MONTH_ENUM);
        return EnumCache.Month_ENUM_VALUES[ordinal];
    }

    public static Month getMonthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(p_addr, OFFSET_MONTH_ENUM);
        return EnumCache.Month_ENUM_VALUES[ordinal];
    }

    public static void setMonth(final long p_cid, final Month p_month) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_MONTH_ENUM, p_month.ordinal());
    }

    public static void setMonthViaAddress(final long p_addr, final Month p_month) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_MONTH_ENUM, p_month.ordinal());
    }

    public static long getCityCityCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_CITY_CID);
    }

    public static long getCityCityAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_CITY_ADDR);
    }

    public static long getCityCityCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_CITY_CID);
    }

    public static long getCityCityAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_CITY_ADDR);
    }

    public static void setCityCityCID(final long p_cid, final long p_city_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_city_cid);
        RAWWRITE.writeLong(addr, OFFSET_CITY_CID, p_city_cid);
        RAWWRITE.writeLong(addr, OFFSET_CITY_ADDR, addr2);
    }

    public static void setCityCityCIDViaAddress(final long p_addr, final long p_city_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_city_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_CITY_CID, p_city_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_CITY_ADDR, addr2);
    }

    public static long getPrimitiveTypes2PrimitiveDataTypesChunkCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_CID);
    }

    public static long getPrimitiveTypes2PrimitiveDataTypesChunkAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PRIMITIVETYPES2_ADDR);
    }

    public static long getPrimitiveTypes2PrimitiveDataTypesChunkCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PRIMITIVETYPES2_CID);
    }

    public static long getPrimitiveTypes2PrimitiveDataTypesChunkAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PRIMITIVETYPES2_ADDR);
    }

    public static void setPrimitiveTypes2PrimitiveDataTypesChunkCID(final long p_cid, final long p_primitivetypes2_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_primitivetypes2_cid);
        RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_CID, p_primitivetypes2_cid);
        RAWWRITE.writeLong(addr, OFFSET_PRIMITIVETYPES2_ADDR, addr2);
    }

    public static void setPrimitiveTypes2PrimitiveDataTypesChunkCIDViaAddress(final long p_addr, final long p_primitivetypes2_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_primitivetypes2_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PRIMITIVETYPES2_CID, p_primitivetypes2_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PRIMITIVETYPES2_ADDR, addr2);
    }

    public static OS getMyOS(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int ordinal = RAWREAD.readInt(addr, OFFSET_MYOS_ENUM);
        return EnumCache.OS_ENUM_VALUES[ordinal];
    }

    public static OS getMyOSViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(p_addr, OFFSET_MYOS_ENUM);
        return EnumCache.OS_ENUM_VALUES[ordinal];
    }

    public static void setMyOS(final long p_cid, final OS p_myos) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_MYOS_ENUM, p_myos.ordinal());
    }

    public static void setMyOSViaAddress(final long p_addr, final OS p_myos) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_MYOS_ENUM, p_myos.ordinal());
    }

    public static HotDrink getFavoriteDrink(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int ordinal = RAWREAD.readInt(addr, OFFSET_FAVORITEDRINK_ENUM);
        return EnumCache.HotDrink_ENUM_VALUES[ordinal];
    }

    public static HotDrink getFavoriteDrinkViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(p_addr, OFFSET_FAVORITEDRINK_ENUM);
        return EnumCache.HotDrink_ENUM_VALUES[ordinal];
    }

    public static void setFavoriteDrink(final long p_cid, final HotDrink p_favoritedrink) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITEDRINK_ENUM, p_favoritedrink.ordinal());
    }

    public static void setFavoriteDrinkViaAddress(final long p_addr, final HotDrink p_favoritedrink) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_FAVORITEDRINK_ENUM, p_favoritedrink.ordinal());
    }

    public static long getMoreStringsStringsChunkCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_MORESTRINGS_CID);
    }

    public static long getMoreStringsStringsChunkAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_MORESTRINGS_ADDR);
    }

    public static long getMoreStringsStringsChunkCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_MORESTRINGS_CID);
    }

    public static long getMoreStringsStringsChunkAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_MORESTRINGS_ADDR);
    }

    public static void setMoreStringsStringsChunkCID(final long p_cid, final long p_morestrings_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_morestrings_cid);
        RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_CID, p_morestrings_cid);
        RAWWRITE.writeLong(addr, OFFSET_MORESTRINGS_ADDR, addr2);
    }

    public static void setMoreStringsStringsChunkCIDViaAddress(final long p_addr, final long p_morestrings_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_morestrings_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_MORESTRINGS_CID, p_morestrings_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_MORESTRINGS_ADDR, addr2);
    }

    public static ProgrammingLanguage getFavoriteLang(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int ordinal = RAWREAD.readInt(addr, OFFSET_FAVORITELANG_ENUM);
        return EnumCache.ProgrammingLanguage_ENUM_VALUES[ordinal];
    }

    public static ProgrammingLanguage getFavoriteLangViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int ordinal = RAWREAD.readInt(p_addr, OFFSET_FAVORITELANG_ENUM);
        return EnumCache.ProgrammingLanguage_ENUM_VALUES[ordinal];
    }

    public static void setFavoriteLang(final long p_cid, final ProgrammingLanguage p_favoritelang) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITELANG_ENUM, p_favoritelang.ordinal());
    }

    public static void setFavoriteLangViaAddress(final long p_addr, final ProgrammingLanguage p_favoritelang) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_FAVORITELANG_ENUM, p_favoritelang.ordinal());
    }

    private DirectComplexChunk() {}

    public static DirectComplexChunk use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        DirectComplexChunk tmp = new DirectComplexChunk();
        tmp.m_addr = PINNING.translate(p_cid);
        return tmp;
    }

    public int getMiscSimpleChunkLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);
    }

    public long getMiscSimpleChunkCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), (8 * p_index));
    }

    public long getMiscSimpleChunkAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), (8 * (len + p_index)));
    }

    public long[] getMiscSimpleChunkCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), 0, len);
    }

    public long[] getMiscSimpleChunkAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR), (8 * len), len);
    }

    public void setMiscSimpleChunkCID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_MISC_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public void setMiscSimpleChunkCIDs(final long[] p_misc) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_MISC_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_MISC_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_MISC_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_MISC_ADDR, 0);
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
        RAWWRITE.writeLongArray(addr2, 0, p_misc);
        for (int i = 0; i < p_misc.length; i ++) {
            final long addr3 = PINNING.translate(p_misc[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_misc.length + i)), addr3);
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

    public long getParentComplexChunkCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PARENT_CID);
    }

    public long getParentComplexChunkAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PARENT_ADDR);
    }

    public void setParentComplexChunkCID(final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PARENT_ADDR, addr2);
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

    public long getAnotherSimpleChunkCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_ANOTHER_CID);
    }

    public long getAnotherSimpleChunkAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_ANOTHER_ADDR);
    }

    public void setAnotherSimpleChunkCID(final long p_another_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_another_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_ANOTHER_CID, p_another_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_ANOTHER_ADDR, addr2);
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

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(m_addr, OFFSET_REFS_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_REFS_ADDR, 0);
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

    public long getChildrenComplexChunkCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public long getChildrenComplexChunkAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * (len + p_index)));
    }

    public long[] getChildrenComplexChunkCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public long[] getChildrenComplexChunkAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * len), len);
    }

    public void setChildrenComplexChunkCID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public void setChildrenComplexChunkCIDs(final long[] p_children) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_CHILDREN_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_CHILDREN_ADDR, 0);
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
        RAWWRITE.writeLongArray(addr2, 0, p_children);
        for (int i = 0; i < p_children.length; i ++) {
            final long addr3 = PINNING.translate(p_children[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_children.length + i)), addr3);
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

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_name == null || p_name.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_NAME_ADDR, 0);
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

    public long getPersonPersonCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PERSON_CID);
    }

    public long getPersonPersonAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PERSON_ADDR);
    }

    public void setPersonPersonCID(final long p_person_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_person_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PERSON_CID, p_person_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PERSON_ADDR, addr2);
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

    public long getCountryCountryCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_COUNTRY_CID);
    }

    public long getCountryCountryAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_COUNTRY_ADDR);
    }

    public void setCountryCountryCID(final long p_country_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_country_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_COUNTRY_CID, p_country_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_COUNTRY_ADDR, addr2);
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

    public long getCityCityCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_CITY_CID);
    }

    public long getCityCityAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_CITY_ADDR);
    }

    public void setCityCityCID(final long p_city_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_city_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_CITY_CID, p_city_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_CITY_ADDR, addr2);
    }

    public long getPrimitiveTypes2PrimitiveDataTypesChunkCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PRIMITIVETYPES2_CID);
    }

    public long getPrimitiveTypes2PrimitiveDataTypesChunkAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PRIMITIVETYPES2_ADDR);
    }

    public void setPrimitiveTypes2PrimitiveDataTypesChunkCID(final long p_primitivetypes2_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_primitivetypes2_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PRIMITIVETYPES2_CID, p_primitivetypes2_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PRIMITIVETYPES2_ADDR, addr2);
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

    public long getMoreStringsStringsChunkCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_MORESTRINGS_CID);
    }

    public long getMoreStringsStringsChunkAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_MORESTRINGS_ADDR);
    }

    public void setMoreStringsStringsChunkCID(final long p_morestrings_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_morestrings_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_MORESTRINGS_CID, p_morestrings_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_MORESTRINGS_ADDR, addr2);
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

        m_addr = 0;
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

        public static boolean getB(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getB(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setB(final long p_cid, final boolean p_b) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setB(addr + OFFSET_TEST1_STRUCT, p_b);
        }

        public static boolean getBViaAddress(final long p_addr) {
            return DirectTestStruct1.getB(p_addr + OFFSET_TEST1_STRUCT);
        }

        public static void setBViaAddress(final long p_addr, final boolean p_b) {
            DirectTestStruct1.setB(p_addr + OFFSET_TEST1_STRUCT, p_b);
        }

        public int getCLength() {
            return DirectTestStruct1.getCLength(m_addr);
        }

        public static int getCLength(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getCLength(addr + OFFSET_TEST1_STRUCT);
        }

        public static int getCLengthViaAddress(final long p_addr) {
            return DirectTestStruct1.getCLength(p_addr + OFFSET_TEST1_STRUCT);
        }

        public char getC(final int p_index) {
            return DirectTestStruct1.getC(m_addr, p_index);
        }

        public static char getC(final long p_cid, final int p_index) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getC(addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public static char getCViaAddress(final long p_addr, final int p_index) {
            return DirectTestStruct1.getC(p_addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public char[] getC() {
            return DirectTestStruct1.getC(m_addr);
        }

        public static char[] getC(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getC(addr + OFFSET_TEST1_STRUCT);
        }

        public static char[] getCViaAddress(final long p_addr) {
            return DirectTestStruct1.getC(p_addr + OFFSET_TEST1_STRUCT);
        }

        public void setC(final int p_index, final char p_value) {
            DirectTestStruct1.setC(m_addr, p_index, p_value);
        }

        public static void setC(final long p_cid, final int p_index, final char p_value) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setC(addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public static void setCViaAddress(final long p_addr, final int p_index, final char p_value) {
            DirectTestStruct1.setC(p_addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public void setC(final char[] p_c) {
            DirectTestStruct1.setC(m_addr, p_c);
        }

        public static void setC(final long p_cid, final char[] p_c) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setC(addr + OFFSET_TEST1_STRUCT, p_c);
        }

        public static void setCViaAddress(final long p_addr, final char[] p_c) {
            DirectTestStruct1.setC(p_addr + OFFSET_TEST1_STRUCT, p_c);
        }

        public String getS() {
            return DirectTestStruct1.getS(m_addr);
        }

        public void setS(final String p_s) {
            DirectTestStruct1.setS(m_addr, p_s);
        }

        public static String getS(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getS(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setS(final long p_cid, final String p_s) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setS(addr + OFFSET_TEST1_STRUCT, p_s);
        }

        public static String getSViaAddress(final long p_addr) {
            return DirectTestStruct1.getS(p_addr + OFFSET_TEST1_STRUCT);
        }

        public static void setSViaAddress(final long p_addr, final String p_s) {
            DirectTestStruct1.setS(p_addr + OFFSET_TEST1_STRUCT, p_s);
        }

        public int getI() {
            return DirectTestStruct1.getI(m_addr);
        }

        public void setI(final int p_i) {
            DirectTestStruct1.setI(m_addr, p_i);
        }

        public static int getI(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getI(addr + OFFSET_TEST1_STRUCT);
        }

        public static void setI(final long p_cid, final int p_i) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setI(addr + OFFSET_TEST1_STRUCT, p_i);
        }

        public static int getIViaAddress(final long p_addr) {
            return DirectTestStruct1.getI(p_addr + OFFSET_TEST1_STRUCT);
        }

        public static void setIViaAddress(final long p_addr, final int p_i) {
            DirectTestStruct1.setI(p_addr + OFFSET_TEST1_STRUCT, p_i);
        }

        public int getDLength() {
            return DirectTestStruct1.getDLength(m_addr);
        }

        public static int getDLength(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getDLength(addr + OFFSET_TEST1_STRUCT);
        }

        public static int getDLengthViaAddress(final long p_addr) {
            return DirectTestStruct1.getDLength(p_addr + OFFSET_TEST1_STRUCT);
        }

        public double getD(final int p_index) {
            return DirectTestStruct1.getD(m_addr, p_index);
        }

        public static double getD(final long p_cid, final int p_index) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getD(addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public static double getDViaAddress(final long p_addr, final int p_index) {
            return DirectTestStruct1.getD(p_addr + OFFSET_TEST1_STRUCT, p_index);
        }

        public double[] getD() {
            return DirectTestStruct1.getD(m_addr);
        }

        public static double[] getD(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct1.getD(addr + OFFSET_TEST1_STRUCT);
        }

        public static double[] getDViaAddress(final long p_addr) {
            return DirectTestStruct1.getD(p_addr + OFFSET_TEST1_STRUCT);
        }

        public void setD(final int p_index, final double p_value) {
            DirectTestStruct1.setD(m_addr, p_index, p_value);
        }

        public static void setD(final long p_cid, final int p_index, final double p_value) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setD(addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public static void setDViaAddress(final long p_addr, final int p_index, final double p_value) {
            DirectTestStruct1.setD(p_addr + OFFSET_TEST1_STRUCT, p_index, p_value);
        }

        public void setD(final double[] p_d) {
            DirectTestStruct1.setD(m_addr, p_d);
        }

        public static void setD(final long p_cid, final double[] p_d) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct1.setD(addr + OFFSET_TEST1_STRUCT, p_d);
        }

        public static void setDViaAddress(final long p_addr, final double[] p_d) {
            DirectTestStruct1.setD(p_addr + OFFSET_TEST1_STRUCT, p_d);
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

        public static String getS(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getS(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setS(final long p_cid, final String p_s) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setS(addr + OFFSET_TEST2_STRUCT, p_s);
        }

        public static String getSViaAddress(final long p_addr) {
            return DirectTestStruct2.getS(p_addr + OFFSET_TEST2_STRUCT);
        }

        public static void setSViaAddress(final long p_addr, final String p_s) {
            DirectTestStruct2.setS(p_addr + OFFSET_TEST2_STRUCT, p_s);
        }

        public int getNumLength() {
            return DirectTestStruct2.getNumLength(m_addr);
        }

        public static int getNumLength(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getNumLength(addr + OFFSET_TEST2_STRUCT);
        }

        public static int getNumLengthViaAddress(final long p_addr) {
            return DirectTestStruct2.getNumLength(p_addr + OFFSET_TEST2_STRUCT);
        }

        public int getNum(final int p_index) {
            return DirectTestStruct2.getNum(m_addr, p_index);
        }

        public static int getNum(final long p_cid, final int p_index) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getNum(addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public static int getNumViaAddress(final long p_addr, final int p_index) {
            return DirectTestStruct2.getNum(p_addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public int[] getNum() {
            return DirectTestStruct2.getNum(m_addr);
        }

        public static int[] getNum(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getNum(addr + OFFSET_TEST2_STRUCT);
        }

        public static int[] getNumViaAddress(final long p_addr) {
            return DirectTestStruct2.getNum(p_addr + OFFSET_TEST2_STRUCT);
        }

        public void setNum(final int p_index, final int p_value) {
            DirectTestStruct2.setNum(m_addr, p_index, p_value);
        }

        public static void setNum(final long p_cid, final int p_index, final int p_value) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setNum(addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public static void setNumViaAddress(final long p_addr, final int p_index, final int p_value) {
            DirectTestStruct2.setNum(p_addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public void setNum(final int[] p_num) {
            DirectTestStruct2.setNum(m_addr, p_num);
        }

        public static void setNum(final long p_cid, final int[] p_num) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setNum(addr + OFFSET_TEST2_STRUCT, p_num);
        }

        public static void setNumViaAddress(final long p_addr, final int[] p_num) {
            DirectTestStruct2.setNum(p_addr + OFFSET_TEST2_STRUCT, p_num);
        }

        public short getX1() {
            return DirectTestStruct2.getX1(m_addr);
        }

        public void setX1(final short p_x1) {
            DirectTestStruct2.setX1(m_addr, p_x1);
        }

        public static short getX1(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getX1(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX1(final long p_cid, final short p_x1) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setX1(addr + OFFSET_TEST2_STRUCT, p_x1);
        }

        public static short getX1ViaAddress(final long p_addr) {
            return DirectTestStruct2.getX1(p_addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX1ViaAddress(final long p_addr, final short p_x1) {
            DirectTestStruct2.setX1(p_addr + OFFSET_TEST2_STRUCT, p_x1);
        }

        public short getX2() {
            return DirectTestStruct2.getX2(m_addr);
        }

        public void setX2(final short p_x2) {
            DirectTestStruct2.setX2(m_addr, p_x2);
        }

        public static short getX2(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getX2(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX2(final long p_cid, final short p_x2) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setX2(addr + OFFSET_TEST2_STRUCT, p_x2);
        }

        public static short getX2ViaAddress(final long p_addr) {
            return DirectTestStruct2.getX2(p_addr + OFFSET_TEST2_STRUCT);
        }

        public static void setX2ViaAddress(final long p_addr, final short p_x2) {
            DirectTestStruct2.setX2(p_addr + OFFSET_TEST2_STRUCT, p_x2);
        }

        public int getAlphaLength() {
            return DirectTestStruct2.getAlphaLength(m_addr);
        }

        public static int getAlphaLength(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getAlphaLength(addr + OFFSET_TEST2_STRUCT);
        }

        public static int getAlphaLengthViaAddress(final long p_addr) {
            return DirectTestStruct2.getAlphaLength(p_addr + OFFSET_TEST2_STRUCT);
        }

        public char getAlpha(final int p_index) {
            return DirectTestStruct2.getAlpha(m_addr, p_index);
        }

        public static char getAlpha(final long p_cid, final int p_index) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getAlpha(addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public static char getAlphaViaAddress(final long p_addr, final int p_index) {
            return DirectTestStruct2.getAlpha(p_addr + OFFSET_TEST2_STRUCT, p_index);
        }

        public char[] getAlpha() {
            return DirectTestStruct2.getAlpha(m_addr);
        }

        public static char[] getAlpha(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getAlpha(addr + OFFSET_TEST2_STRUCT);
        }

        public static char[] getAlphaViaAddress(final long p_addr) {
            return DirectTestStruct2.getAlpha(p_addr + OFFSET_TEST2_STRUCT);
        }

        public void setAlpha(final int p_index, final char p_value) {
            DirectTestStruct2.setAlpha(m_addr, p_index, p_value);
        }

        public static void setAlpha(final long p_cid, final int p_index, final char p_value) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setAlpha(addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public static void setAlphaViaAddress(final long p_addr, final int p_index, final char p_value) {
            DirectTestStruct2.setAlpha(p_addr + OFFSET_TEST2_STRUCT, p_index, p_value);
        }

        public void setAlpha(final char[] p_alpha) {
            DirectTestStruct2.setAlpha(m_addr, p_alpha);
        }

        public static void setAlpha(final long p_cid, final char[] p_alpha) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setAlpha(addr + OFFSET_TEST2_STRUCT, p_alpha);
        }

        public static void setAlphaViaAddress(final long p_addr, final char[] p_alpha) {
            DirectTestStruct2.setAlpha(p_addr + OFFSET_TEST2_STRUCT, p_alpha);
        }

        public short getY1() {
            return DirectTestStruct2.getY1(m_addr);
        }

        public void setY1(final short p_y1) {
            DirectTestStruct2.setY1(m_addr, p_y1);
        }

        public static short getY1(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getY1(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY1(final long p_cid, final short p_y1) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setY1(addr + OFFSET_TEST2_STRUCT, p_y1);
        }

        public static short getY1ViaAddress(final long p_addr) {
            return DirectTestStruct2.getY1(p_addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY1ViaAddress(final long p_addr, final short p_y1) {
            DirectTestStruct2.setY1(p_addr + OFFSET_TEST2_STRUCT, p_y1);
        }

        public short getY2() {
            return DirectTestStruct2.getY2(m_addr);
        }

        public void setY2(final short p_y2) {
            DirectTestStruct2.setY2(m_addr, p_y2);
        }

        public static short getY2(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectTestStruct2.getY2(addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY2(final long p_cid, final short p_y2) {
            final long addr = PINNING.translate(p_cid);
            DirectTestStruct2.setY2(addr + OFFSET_TEST2_STRUCT, p_y2);
        }

        public static short getY2ViaAddress(final long p_addr) {
            return DirectTestStruct2.getY2(p_addr + OFFSET_TEST2_STRUCT);
        }

        public static void setY2ViaAddress(final long p_addr, final short p_y2) {
            DirectTestStruct2.setY2(p_addr + OFFSET_TEST2_STRUCT, p_y2);
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

        public static boolean getB(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getB(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setB(final long p_cid, final boolean p_b) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setB(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b);
        }

        public static boolean getBViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getB(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setBViaAddress(final long p_addr, final boolean p_b) {
            DirectPrimitiveDataTypesStruct.setB(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b);
        }

        public byte getB2() {
            return DirectPrimitiveDataTypesStruct.getB2(m_addr);
        }

        public void setB2(final byte p_b2) {
            DirectPrimitiveDataTypesStruct.setB2(m_addr, p_b2);
        }

        public static byte getB2(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getB2(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setB2(final long p_cid, final byte p_b2) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setB2(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b2);
        }

        public static byte getB2ViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getB2(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setB2ViaAddress(final long p_addr, final byte p_b2) {
            DirectPrimitiveDataTypesStruct.setB2(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_b2);
        }

        public char getC() {
            return DirectPrimitiveDataTypesStruct.getC(m_addr);
        }

        public void setC(final char p_c) {
            DirectPrimitiveDataTypesStruct.setC(m_addr, p_c);
        }

        public static char getC(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getC(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setC(final long p_cid, final char p_c) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setC(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_c);
        }

        public static char getCViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getC(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setCViaAddress(final long p_addr, final char p_c) {
            DirectPrimitiveDataTypesStruct.setC(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_c);
        }

        public double getD() {
            return DirectPrimitiveDataTypesStruct.getD(m_addr);
        }

        public void setD(final double p_d) {
            DirectPrimitiveDataTypesStruct.setD(m_addr, p_d);
        }

        public static double getD(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getD(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setD(final long p_cid, final double p_d) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setD(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_d);
        }

        public static double getDViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getD(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setDViaAddress(final long p_addr, final double p_d) {
            DirectPrimitiveDataTypesStruct.setD(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_d);
        }

        public float getF() {
            return DirectPrimitiveDataTypesStruct.getF(m_addr);
        }

        public void setF(final float p_f) {
            DirectPrimitiveDataTypesStruct.setF(m_addr, p_f);
        }

        public static float getF(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getF(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setF(final long p_cid, final float p_f) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setF(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_f);
        }

        public static float getFViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getF(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setFViaAddress(final long p_addr, final float p_f) {
            DirectPrimitiveDataTypesStruct.setF(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_f);
        }

        public int getNum() {
            return DirectPrimitiveDataTypesStruct.getNum(m_addr);
        }

        public void setNum(final int p_num) {
            DirectPrimitiveDataTypesStruct.setNum(m_addr, p_num);
        }

        public static int getNum(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getNum(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setNum(final long p_cid, final int p_num) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setNum(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_num);
        }

        public static int getNumViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getNum(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setNumViaAddress(final long p_addr, final int p_num) {
            DirectPrimitiveDataTypesStruct.setNum(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_num);
        }

        public long getBignum() {
            return DirectPrimitiveDataTypesStruct.getBignum(m_addr);
        }

        public void setBignum(final long p_bignum) {
            DirectPrimitiveDataTypesStruct.setBignum(m_addr, p_bignum);
        }

        public static long getBignum(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getBignum(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setBignum(final long p_cid, final long p_bignum) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setBignum(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_bignum);
        }

        public static long getBignumViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getBignum(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setBignumViaAddress(final long p_addr, final long p_bignum) {
            DirectPrimitiveDataTypesStruct.setBignum(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_bignum);
        }

        public short getS() {
            return DirectPrimitiveDataTypesStruct.getS(m_addr);
        }

        public void setS(final short p_s) {
            DirectPrimitiveDataTypesStruct.setS(m_addr, p_s);
        }

        public static short getS(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectPrimitiveDataTypesStruct.getS(addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setS(final long p_cid, final short p_s) {
            final long addr = PINNING.translate(p_cid);
            DirectPrimitiveDataTypesStruct.setS(addr + OFFSET_PRIMITIVETYPES_STRUCT, p_s);
        }

        public static short getSViaAddress(final long p_addr) {
            return DirectPrimitiveDataTypesStruct.getS(p_addr + OFFSET_PRIMITIVETYPES_STRUCT);
        }

        public static void setSViaAddress(final long p_addr, final short p_s) {
            DirectPrimitiveDataTypesStruct.setS(p_addr + OFFSET_PRIMITIVETYPES_STRUCT, p_s);
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

        public static String getS1(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectStringsStruct.getS1(addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS1(final long p_cid, final String p_s1) {
            final long addr = PINNING.translate(p_cid);
            DirectStringsStruct.setS1(addr + OFFSET_MORESTRINGS2_STRUCT, p_s1);
        }

        public static String getS1ViaAddress(final long p_addr) {
            return DirectStringsStruct.getS1(p_addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS1ViaAddress(final long p_addr, final String p_s1) {
            DirectStringsStruct.setS1(p_addr + OFFSET_MORESTRINGS2_STRUCT, p_s1);
        }

        public String getS2() {
            return DirectStringsStruct.getS2(m_addr);
        }

        public void setS2(final String p_s2) {
            DirectStringsStruct.setS2(m_addr, p_s2);
        }

        public static String getS2(final long p_cid) {
            final long addr = PINNING.translate(p_cid);
            return DirectStringsStruct.getS2(addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS2(final long p_cid, final String p_s2) {
            final long addr = PINNING.translate(p_cid);
            DirectStringsStruct.setS2(addr + OFFSET_MORESTRINGS2_STRUCT, p_s2);
        }

        public static String getS2ViaAddress(final long p_addr) {
            return DirectStringsStruct.getS2(p_addr + OFFSET_MORESTRINGS2_STRUCT);
        }

        public static void setS2ViaAddress(final long p_addr, final String p_s2) {
            DirectStringsStruct.setS2(p_addr + OFFSET_MORESTRINGS2_STRUCT, p_s2);
        }
    }
}
