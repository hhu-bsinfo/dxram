package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectSimpleChunk implements AutoCloseable {

    private static final int OFFSET_ID = 0;
    private static final int OFFSET_NAME_LENGTH = 8;
    private static final int OFFSET_NAME_CID = 12;
    private static final int OFFSET_NAME_ADDR = 20;
    private static final int OFFSET_NUMBERS_LENGTH = 28;
    private static final int OFFSET_NUMBERS_CID = 32;
    private static final int OFFSET_NUMBERS_ADDR = 40;
    private static final int OFFSET_PARENT_CID = 48;
    private static final int OFFSET_PARENT_ADDR = 56;
    private static final int OFFSET_CHILDREN_LENGTH = 64;
    private static final int OFFSET_CHILDREN_CID = 68;
    private static final int OFFSET_CHILDREN_ADDR = 76;
    private static final int SIZE = 84;
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
        RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_NUMBERS_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_NUMBERS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_NUMBERS_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_NUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_NUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_CHILDREN_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CHILDREN_ADDR, 0);
        }
    }

    public static void remove(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
        PINNING.unpinCID(cid_OFFSET_NAME_CID);
        REMOVE.remove(cid_OFFSET_NAME_CID);
        final long cid_OFFSET_NUMBERS_CID = RAWREAD.readLong(addr, OFFSET_NUMBERS_CID);
        PINNING.unpinCID(cid_OFFSET_NUMBERS_CID);
        REMOVE.remove(cid_OFFSET_NUMBERS_CID);
        final long cid_OFFSET_PARENT_CID = RAWREAD.readLong(addr, OFFSET_PARENT_CID);
        PINNING.unpinCID(cid_OFFSET_PARENT_CID);
        REMOVE.remove(cid_OFFSET_PARENT_CID);
        final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
        PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
        REMOVE.remove(cid_OFFSET_CHILDREN_CID);
        PINNING.unpinCID(p_cid);
        REMOVE.remove(p_cid);
    }

    public static void remove(final long[] p_cids) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        for (int i = 0; i < p_cids.length; i ++) {
            final long addr = PINNING.translate(p_cids[i]);
            final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
            PINNING.unpinCID(cid_OFFSET_NAME_CID);
            REMOVE.remove(cid_OFFSET_NAME_CID);
            final long cid_OFFSET_NUMBERS_CID = RAWREAD.readLong(addr, OFFSET_NUMBERS_CID);
            PINNING.unpinCID(cid_OFFSET_NUMBERS_CID);
            REMOVE.remove(cid_OFFSET_NUMBERS_CID);
            final long cid_OFFSET_PARENT_CID = RAWREAD.readLong(addr, OFFSET_PARENT_CID);
            PINNING.unpinCID(cid_OFFSET_PARENT_CID);
            REMOVE.remove(cid_OFFSET_PARENT_CID);
            final long cid_OFFSET_CHILDREN_CID = RAWREAD.readLong(addr, OFFSET_CHILDREN_CID);
            PINNING.unpinCID(cid_OFFSET_CHILDREN_CID);
            REMOVE.remove(cid_OFFSET_CHILDREN_CID);
            PINNING.unpinCID(p_cids[i]);
            REMOVE.remove(p_cids[i]);
        }
    }

    public static long getId(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_ID);
    }

    public static long getIdViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_ID);
    }

    public static void setId(final long p_cid, final long p_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeLong(addr, OFFSET_ID, p_id);
    }

    public static void setIdViaAddress(final long p_addr, final long p_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(p_addr, OFFSET_ID, p_id);
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

    public static int getNumbersLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_NUMBERS_LENGTH);
    }

    public static int getNumbersLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_NUMBERS_LENGTH);
    }

    public static int getNumbers(final long p_cid, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_NUMBERS_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    public static int getNumbersViaAddress(final long p_addr, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_NUMBERS_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    public static int[] getNumbers(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(addr, OFFSET_NUMBERS_ADDR), 0, len);
    }

    public static int[] getNumbersViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(p_addr, OFFSET_NUMBERS_ADDR), 0, len);
    }

    public static void setNumbers(final long p_cid, final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_NUMBERS_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    public static void setNumbersViaAddress(final long p_addr, final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_NUMBERS_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    public static void setNumbers(final long p_cid, final int[] p_numbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_NUMBERS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_NUMBERS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_NUMBERS_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_NUMBERS_LENGTH, 0);
        }

        if (p_numbers == null || p_numbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_numbers.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_NUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_NUMBERS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_NUMBERS_LENGTH, p_numbers.length);
        RAWWRITE.writeIntArray(addr2, 0, p_numbers);
    }

    public static void setNumbersViaAddress(final long p_addr, final int[] p_numbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_NUMBERS_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_NUMBERS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_NUMBERS_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_NUMBERS_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_NUMBERS_LENGTH, 0);
        }

        if (p_numbers == null || p_numbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_numbers.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_NUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_NUMBERS_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_NUMBERS_LENGTH, p_numbers.length);
        RAWWRITE.writeIntArray(addr2, 0, p_numbers);
    }

    public static long getParentSimpleChunkCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PARENT_CID);
    }

    public static long getParentSimpleChunkAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PARENT_ADDR);
    }

    public static long getParentSimpleChunkCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PARENT_CID);
    }

    public static long getParentSimpleChunkAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PARENT_ADDR);
    }

    public static void setParentSimpleChunkCID(final long p_cid, final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(addr, OFFSET_PARENT_ADDR, addr2);
    }

    public static void setParentSimpleChunkCIDViaAddress(final long p_addr, final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PARENT_ADDR, addr2);
    }

    public static int getChildrenSimpleChunkLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_CHILDREN_LENGTH);
    }

    public static long getChildrenSimpleChunkCID(final long p_cid, final int p_index) {
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

    public static long getChildrenSimpleChunkAddress(final long p_cid, final int p_index) {
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

    public static long[] getChildrenSimpleChunkCIDs(final long p_cid) {
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

    public static long[] getChildrenSimpleChunkAddresses(final long p_cid) {
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

    public static int getChildrenSimpleChunkLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);
    }

    public static long getChildrenSimpleChunkCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public static long getChildrenSimpleChunkAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * (len + p_index)));
    }

    public static long[] getChildrenSimpleChunkCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public static long[] getChildrenSimpleChunkAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CHILDREN_ADDR), (8 * len), len);
    }

    public static void setChildrenSimpleChunkCID(final long p_cid, final int p_index, final long p_value) {
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

    public static void setChildrenSimpleChunkCIDs(final long p_cid, final long[] p_children) {
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

    public static void setChildrenSimpleChunkCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
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

    public static void setChildrenSimpleChunkCIDsViaAddress(final long p_addr, final long[] p_children) {
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

    private DirectSimpleChunk() {}

    public static DirectSimpleChunk use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        DirectSimpleChunk tmp = new DirectSimpleChunk();
        tmp.m_addr = PINNING.translate(p_cid);
        return tmp;
    }

    public long getId() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_ID);
    }

    public void setId(final long p_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(m_addr, OFFSET_ID, p_id);
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

    public int getNumbersLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_NUMBERS_LENGTH);
    }

    public int getNumbers(final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_NUMBERS_ADDR);
        return RAWREAD.readInt(addr, (4 * index));
    }

    public int[] getNumbers() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(m_addr, OFFSET_NUMBERS_ADDR), 0, len);
    }

    public void setNumbers(final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_NUMBERS_ADDR);
        RAWWRITE.writeInt(addr, (4 * index), value);
    }

    public void setNumbers(final int[] p_numbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_NUMBERS_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_NUMBERS_CID);

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(m_addr, OFFSET_NUMBERS_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_NUMBERS_ADDR, 0);
            RAWWRITE.writeInt(m_addr, OFFSET_NUMBERS_LENGTH, 0);
        }

        if (p_numbers == null || p_numbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_numbers.length));
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_NUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_NUMBERS_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_NUMBERS_LENGTH, p_numbers.length);
        RAWWRITE.writeIntArray(addr, 0, p_numbers);
    }

    public long getParentSimpleChunkCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PARENT_CID);
    }

    public long getParentSimpleChunkAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PARENT_ADDR);
    }

    public void setParentSimpleChunkCID(final long p_parent_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_parent_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PARENT_CID, p_parent_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PARENT_ADDR, addr2);
    }

    public int getChildrenSimpleChunkLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);
    }

    public long getChildrenSimpleChunkCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * p_index));
    }

    public long getChildrenSimpleChunkAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * (len + p_index)));
    }

    public long[] getChildrenSimpleChunkCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), 0, len);
    }

    public long[] getChildrenSimpleChunkAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CHILDREN_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CHILDREN_ADDR), (8 * len), len);
    }

    public void setChildrenSimpleChunkCID(final int p_index, final long p_value) {
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

    public void setChildrenSimpleChunkCIDs(final long[] p_children) {
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

    @Override
    public void close() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        m_addr = 0;
    }
}
