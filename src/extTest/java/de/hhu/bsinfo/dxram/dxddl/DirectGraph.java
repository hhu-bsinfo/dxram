package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectGraph implements AutoCloseable {

    private static final int HEADER_LID = 0;
    private static final int HEADER_TYPE = 6;
    private static final int OFFSET_NAME_LENGTH = 8;
    private static final int OFFSET_NAME_CID = 12;
    private static final int OFFSET_NAME_ADDR = 20;
    private static final int OFFSET_VERSION = 28;
    private static final int OFFSET_EDGELIST_LENGTH = 36;
    private static final int OFFSET_EDGELIST_CID = 40;
    private static final int OFFSET_EDGELIST_ADDR = 48;
    private static final int SIZE = 56;
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
        RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_EDGELIST_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_EDGELIST_LENGTH, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_EDGELIST_ADDR, 0x0L);
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
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EDGELIST_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_ADDR, 0x0L);
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
            RAWWRITE.writeLong(addr, OFFSET_NAME_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EDGELIST_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_ADDR, 0x0L);
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

        final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
        PINNING.unpinCID(cid_OFFSET_NAME_CID);
        REMOVE.remove(cid_OFFSET_NAME_CID);
        final long cid_OFFSET_EDGELIST_CID = RAWREAD.readLong(addr, OFFSET_EDGELIST_CID);
        PINNING.unpinCID(cid_OFFSET_EDGELIST_CID);
        REMOVE.remove(cid_OFFSET_EDGELIST_CID);
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

            final long cid_OFFSET_NAME_CID = RAWREAD.readLong(addr, OFFSET_NAME_CID);
            PINNING.unpinCID(cid_OFFSET_NAME_CID);
            REMOVE.remove(cid_OFFSET_NAME_CID);
            final long cid_OFFSET_EDGELIST_CID = RAWREAD.readLong(addr, OFFSET_EDGELIST_CID);
            PINNING.unpinCID(cid_OFFSET_EDGELIST_CID);
            REMOVE.remove(cid_OFFSET_EDGELIST_CID);
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
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

    public static long getVersion(final long p_id) {
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

        return RAWREAD.readLong(addr, OFFSET_VERSION);
    }

    public static void setVersion(final long p_id, final long p_version) {
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

        RAWWRITE.writeLong(addr, OFFSET_VERSION, p_version);
    }

    public static int getEdgeListEdgeLength(final long p_id) {
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

        return RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);
    }

    public static long getEdgeListEdgeID(final long p_id, final int p_index) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_EDGELIST_ADDR), (8 * p_index));
    }

    public static long[] getEdgeListEdgeIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_EDGELIST_ADDR), 0, len);
    }

    public static long[] getEdgeListEdgeLocalIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_EDGELIST_ADDR), 0, len);
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

    public static long[] getEdgeListEdgeRemoteIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_EDGELIST_ADDR), 0, len);
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

    public static void setEdgeListEdgeID(final long p_id, final int p_index, final long p_value) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_EDGELIST_ADDR);
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

    public static void setEdgeListEdgeIDs(final long p_id, final long[] p_edgelist) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EDGELIST_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_EDGELIST_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EDGELIST_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_EDGELIST_LENGTH, 0);
        }

        if (p_edgelist == null || p_edgelist.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_edgelist.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_EDGELIST_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_EDGELIST_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_EDGELIST_LENGTH, p_edgelist.length);
        for (int i = 0; i < p_edgelist.length; i ++) {
            if ((p_edgelist[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_edgelist[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_edgelist[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_edgelist[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_edgelist[i]);
            }
        }
    }

    private DirectGraph() {}

    public static DirectGraph use(final long p_id) {
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

        DirectGraph tmp = new DirectGraph();
        tmp.m_addr = addr;
        return tmp;
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

    public long getVersion() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_VERSION);
    }

    public void setVersion(final long p_version) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(m_addr, OFFSET_VERSION, p_version);
    }

    public int getEdgeListEdgeLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);
    }

    public long getEdgeListEdgeID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_EDGELIST_ADDR), (8 * p_index));
    }

    public long[] getEdgeListEdgeIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_EDGELIST_ADDR), 0, len);
    }

    public long[] getEdgeListEdgeLocalIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_EDGELIST_ADDR), 0, len);
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

    public long[] getEdgeListEdgeRemoteIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_EDGELIST_ADDR), 0, len);
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

    public void setEdgeListEdgeID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_EDGELIST_ADDR);
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

    public void setEdgeListEdgeIDs(final long[] p_edgelist) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EDGELIST_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_EDGELIST_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_EDGELIST_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_EDGELIST_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_EDGELIST_LENGTH, 0);
        }

        if (p_edgelist == null || p_edgelist.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_edgelist.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_EDGELIST_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_EDGELIST_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_EDGELIST_LENGTH, p_edgelist.length);
        for (int i = 0; i < p_edgelist.length; i ++) {
            if ((p_edgelist[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_edgelist[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_edgelist[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_edgelist[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_edgelist[i]);
            }
        }
    }

    @Override
    public void close() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        m_addr = 0x0L;
    }
}
