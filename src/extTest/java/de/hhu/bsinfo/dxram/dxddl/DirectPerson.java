package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectPerson implements AutoCloseable {

    private static final int HEADER_LID = 0;
    private static final int HEADER_TYPE = 6;
    private static final int OFFSET_NAME_LENGTH = 8;
    private static final int OFFSET_NAME_CID = 12;
    private static final int OFFSET_NAME_ADDR = 20;
    private static final int OFFSET_AGE = 28;
    private static final int OFFSET_DATEOFBIRTH = 30;
    private static final int OFFSET_PLACEOFBIRTH_ID = 38;
    private static final int OFFSET_HOMEADDRESS_LENGTH = 46;
    private static final int OFFSET_HOMEADDRESS_CID = 50;
    private static final int OFFSET_HOMEADDRESS_ADDR = 58;
    private static final int OFFSET_EMAIL_LENGTH = 66;
    private static final int OFFSET_EMAIL_CID = 70;
    private static final int OFFSET_EMAIL_ADDR = 78;
    private static final int OFFSET_FAVORITENUMBERS_LENGTH = 86;
    private static final int OFFSET_FAVORITENUMBERS_CID = 90;
    private static final int OFFSET_FAVORITENUMBERS_ADDR = 98;
    private static final int OFFSET_FRIENDS_LENGTH = 106;
    private static final int OFFSET_FRIENDS_CID = 110;
    private static final int OFFSET_FRIENDS_ADDR = 118;
    private static final int OFFSET_FAMILY_LENGTH = 126;
    private static final int OFFSET_FAMILY_CID = 130;
    private static final int OFFSET_FAMILY_ADDR = 138;
    private static final int SIZE = 146;
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
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, 0xFFFFFFFFFFFFFFFFL);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0x0L);
        RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, 0xFFFFFFFFFFFFFFFFL);
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
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, 0xFFFFFFFFFFFFFFFFL);
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
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0x0L);
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, 0xFFFFFFFFFFFFFFFFL);
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
        final long cid_OFFSET_HOMEADDRESS_CID = RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_CID);
        PINNING.unpinCID(cid_OFFSET_HOMEADDRESS_CID);
        REMOVE.remove(cid_OFFSET_HOMEADDRESS_CID);
        final long cid_OFFSET_EMAIL_CID = RAWREAD.readLong(addr, OFFSET_EMAIL_CID);
        PINNING.unpinCID(cid_OFFSET_EMAIL_CID);
        REMOVE.remove(cid_OFFSET_EMAIL_CID);
        final long cid_OFFSET_FAVORITENUMBERS_CID = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_CID);
        PINNING.unpinCID(cid_OFFSET_FAVORITENUMBERS_CID);
        REMOVE.remove(cid_OFFSET_FAVORITENUMBERS_CID);
        final long cid_OFFSET_FRIENDS_CID = RAWREAD.readLong(addr, OFFSET_FRIENDS_CID);
        PINNING.unpinCID(cid_OFFSET_FRIENDS_CID);
        REMOVE.remove(cid_OFFSET_FRIENDS_CID);
        final long cid_OFFSET_FAMILY_CID = RAWREAD.readLong(addr, OFFSET_FAMILY_CID);
        PINNING.unpinCID(cid_OFFSET_FAMILY_CID);
        REMOVE.remove(cid_OFFSET_FAMILY_CID);
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
            final long cid_OFFSET_HOMEADDRESS_CID = RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_CID);
            PINNING.unpinCID(cid_OFFSET_HOMEADDRESS_CID);
            REMOVE.remove(cid_OFFSET_HOMEADDRESS_CID);
            final long cid_OFFSET_EMAIL_CID = RAWREAD.readLong(addr, OFFSET_EMAIL_CID);
            PINNING.unpinCID(cid_OFFSET_EMAIL_CID);
            REMOVE.remove(cid_OFFSET_EMAIL_CID);
            final long cid_OFFSET_FAVORITENUMBERS_CID = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_CID);
            PINNING.unpinCID(cid_OFFSET_FAVORITENUMBERS_CID);
            REMOVE.remove(cid_OFFSET_FAVORITENUMBERS_CID);
            final long cid_OFFSET_FRIENDS_CID = RAWREAD.readLong(addr, OFFSET_FRIENDS_CID);
            PINNING.unpinCID(cid_OFFSET_FRIENDS_CID);
            REMOVE.remove(cid_OFFSET_FRIENDS_CID);
            final long cid_OFFSET_FAMILY_CID = RAWREAD.readLong(addr, OFFSET_FAMILY_CID);
            PINNING.unpinCID(cid_OFFSET_FAMILY_CID);
            REMOVE.remove(cid_OFFSET_FAMILY_CID);
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

    public static short getAge(final long p_id) {
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

        return RAWREAD.readShort(addr, OFFSET_AGE);
    }

    public static void setAge(final long p_id, final short p_age) {
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

        RAWWRITE.writeShort(addr, OFFSET_AGE, p_age);
    }

    public static long getDateOfBirth(final long p_id) {
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

        return RAWREAD.readLong(addr, OFFSET_DATEOFBIRTH);
    }

    public static void setDateOfBirth(final long p_id, final long p_dateofbirth) {
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

        RAWWRITE.writeLong(addr, OFFSET_DATEOFBIRTH, p_dateofbirth);
    }

    public static long getPlaceOfBirthCityID(final long p_id) {
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

        return RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_ID);
    }

    public static boolean isPlaceOfBirthCityLocalID(final long p_id) {
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

        final long id2 = RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_ID);
        return (id2 & 0xFFFF000000000000L) == 0xFFFF000000000000L;
    }

    public static void setPlaceOfBirthCityID(final long p_id, final long p_placeofbirth_id) {
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

        if ((p_placeofbirth_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_placeofbirth_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, p_placeofbirth_id);
            } else {
                RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, PINNING.translate(p_placeofbirth_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ID, p_placeofbirth_id);
        }
    }

    public static String getHomeAddress(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_HOMEADDRESS_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_ADDR), 0, len));
    }

    public static void setHomeAddress(final long p_id, final String p_homeaddress) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_HOMEADDRESS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_homeaddress == null || p_homeaddress.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, (p_homeaddress == null ? -1 : 0));
            return;
        }

        final byte[] str = p_homeaddress.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static String getEmail(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EMAIL_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_EMAIL_ADDR), 0, len));
    }

    public static void setEmail(final long p_id, final String p_email) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_EMAIL_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_EMAIL_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_email == null || p_email.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, (p_email == null ? -1 : 0));
            return;
        }

        final byte[] str = p_email.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static int getFavoriteNumbersLength(final long p_id) {
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

        return RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);
    }

    public static int getFavoriteNumbers(final long p_id, final int index) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    public static int[] getFavoriteNumbers(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR), 0, len);
    }

    public static void setFavoriteNumbers(final long p_id, final int index, final int value) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    public static void setFavoriteNumbers(final long p_id, final int[] p_favoritenumbers) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
        }

        if (p_favoritenumbers == null || p_favoritenumbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_favoritenumbers.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, p_favoritenumbers.length);
        RAWWRITE.writeIntArray(addr2, 0, p_favoritenumbers);
    }

    public static int getFriendsPersonLength(final long p_id) {
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

        return RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);
    }

    public static long getFriendsPersonID(final long p_id, final int p_index) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), (8 * p_index));
    }

    public static long[] getFriendsPersonIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), 0, len);
    }

    public static long[] getFriendsPersonLocalIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), 0, len);
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

    public static long[] getFriendsPersonRemoteIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), 0, len);
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

    public static void setFriendsPersonID(final long p_id, final int p_index, final long p_value) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR);
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

    public static void setFriendsPersonIDs(final long p_id, final long[] p_friends) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FRIENDS_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
        }

        if (p_friends == null || p_friends.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_friends.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, p_friends.length);
        for (int i = 0; i < p_friends.length; i ++) {
            if ((p_friends[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_friends[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_friends[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_friends[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_friends[i]);
            }
        }
    }

    public static int getFamilyPersonLength(final long p_id) {
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

        return RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);
    }

    public static long getFamilyPersonID(final long p_id, final int p_index) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), (8 * p_index));
    }

    public static long[] getFamilyPersonIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), 0, len);
    }

    public static long[] getFamilyPersonLocalIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), 0, len);
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

    public static long[] getFamilyPersonRemoteIDs(final long p_id) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), 0, len);
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

    public static void setFamilyPersonID(final long p_id, final int p_index, final long p_value) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR);
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

    public static void setFamilyPersonIDs(final long p_id, final long[] p_family) {
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

        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FAMILY_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0x0L);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
        }

        if (p_family == null || p_family.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (8 * p_family.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, p_family.length);
        for (int i = 0; i < p_family.length; i ++) {
            if ((p_family[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_family[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_family[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_family[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_family[i]);
            }
        }
    }

    private DirectPerson() {}

    public static DirectPerson use(final long p_id) {
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

        DirectPerson tmp = new DirectPerson();
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

    public short getAge() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readShort(m_addr, OFFSET_AGE);
    }

    public void setAge(final short p_age) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeShort(m_addr, OFFSET_AGE, p_age);
    }

    public long getDateOfBirth() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_DATEOFBIRTH);
    }

    public void setDateOfBirth(final long p_dateofbirth) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(m_addr, OFFSET_DATEOFBIRTH, p_dateofbirth);
    }

    public long getPlaceOfBirthCityID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PLACEOFBIRTH_ID);
    }

    public void setPlaceOfBirthCityID(final long p_placeofbirth_id) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        if ((p_placeofbirth_id & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
            if ((p_placeofbirth_id & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                RAWWRITE.writeLong(m_addr, OFFSET_PLACEOFBIRTH_ID, p_placeofbirth_id);
            } else {
                RAWWRITE.writeLong(m_addr, OFFSET_PLACEOFBIRTH_ID, PINNING.translate(p_placeofbirth_id) | 0xFFFF000000000000L);
            }
        } else {
            RAWWRITE.writeLong(m_addr, OFFSET_PLACEOFBIRTH_ID, p_placeofbirth_id);
        }
    }

    public String getHomeAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_HOMEADDRESS_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(m_addr, OFFSET_HOMEADDRESS_ADDR), 0, len));
    }

    public void setHomeAddress(final String p_homeaddress) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_HOMEADDRESS_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_HOMEADDRESS_CID);

        if (cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_homeaddress == null || p_homeaddress.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_HOMEADDRESS_LENGTH, (p_homeaddress == null ? -1 : 0));
            return;
        }

        final byte[] str = p_homeaddress.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_HOMEADDRESS_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr, 0, str);
    }

    public String getEmail() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EMAIL_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(m_addr, OFFSET_EMAIL_ADDR), 0, len));
    }

    public void setEmail(final String p_email) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_EMAIL_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_EMAIL_CID);

        if (cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_email == null || p_email.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_EMAIL_LENGTH, (p_email == null ? -1 : 0));
            return;
        }

        final byte[] str = p_email.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_EMAIL_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr, 0, str);
    }

    public int getFavoriteNumbersLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH);
    }

    public int getFavoriteNumbers(final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR);
        return RAWREAD.readInt(addr, (4 * index));
    }

    public int[] getFavoriteNumbers() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR), 0, len);
    }

    public void setFavoriteNumbers(final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr = RAWREAD.readLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR);
        RAWWRITE.writeInt(addr, (4 * index), value);
    }

    public void setFavoriteNumbers(final int[] p_favoritenumbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH);
        final long cid = RAWREAD.readLong(m_addr, OFFSET_FAVORITENUMBERS_CID);

        if (cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
        }

        if (p_favoritenumbers == null || p_favoritenumbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_favoritenumbers.length));
        final long addr = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR, addr);
        RAWWRITE.writeInt(m_addr, OFFSET_FAVORITENUMBERS_LENGTH, p_favoritenumbers.length);
        RAWWRITE.writeIntArray(addr, 0, p_favoritenumbers);
    }

    public int getFriendsPersonLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);
    }

    public long getFriendsPersonID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), (8 * p_index));
    }

    public long[] getFriendsPersonIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), 0, len);
    }

    public long[] getFriendsPersonLocalIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), 0, len);
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

    public long[] getFriendsPersonRemoteIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), 0, len);
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

    public void setFriendsPersonID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR);
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

    public void setFriendsPersonIDs(final long[] p_friends) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_FRIENDS_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_FRIENDS_LENGTH, 0);
        }

        if (p_friends == null || p_friends.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_friends.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_FRIENDS_LENGTH, p_friends.length);
        for (int i = 0; i < p_friends.length; i ++) {
            if ((p_friends[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_friends[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_friends[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_friends[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_friends[i]);
            }
        }
    }

    public int getFamilyPersonLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);
    }

    public long getFamilyPersonID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), (8 * p_index));
    }

    public long[] getFamilyPersonIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), 0, len);
    }

    public long[] getFamilyPersonLocalIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), 0, len);
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

    public long[] getFamilyPersonRemoteIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        final long[] ids = RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), 0, len);
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

    public void setFamilyPersonID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR);
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

    public void setFamilyPersonIDs(final long[] p_family) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_FAMILY_CID);

        if (array_cid != 0xFFFFFFFFFFFFFFFFL) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_CID, 0xFFFFFFFFFFFFFFFFL);
            RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_ADDR, 0x0L);
            RAWWRITE.writeInt(m_addr, OFFSET_FAMILY_LENGTH, 0);
        }

        if (p_family == null || p_family.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_family.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_FAMILY_LENGTH, p_family.length);
        for (int i = 0; i < p_family.length; i ++) {
            if ((p_family[i] & 0xFFFF000000000000L) != 0xFFFF000000000000L) {
                if ((p_family[i] & 0xFFFF000000000000L) != DirectAccessSecurityManager.NID) {
                    RAWWRITE.writeLong(addr2, (8 * i), p_family[i]);
                } else {
                    RAWWRITE.writeLong(addr2, (8 * i), PINNING.translate(p_family[i]) | 0xFFFF000000000000L);
                }
            } else {
                RAWWRITE.writeLong(addr2, (8 * i), p_family[i]);
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
