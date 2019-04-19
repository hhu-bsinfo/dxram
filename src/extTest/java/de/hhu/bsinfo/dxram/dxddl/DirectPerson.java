package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectPerson implements AutoCloseable {

    private static final int OFFSET_NAME_LENGTH = 0;
    private static final int OFFSET_NAME_CID = 4;
    private static final int OFFSET_NAME_ADDR = 12;
    private static final int OFFSET_AGE = 20;
    private static final int OFFSET_DATEOFBIRTH = 22;
    private static final int OFFSET_PLACEOFBIRTH_CID = 30;
    private static final int OFFSET_PLACEOFBIRTH_ADDR = 38;
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
        RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, -1);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
        RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_HOMEADDRESS_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_EMAIL_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0);
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
        final long cid_OFFSET_PLACEOFBIRTH_CID = RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_CID);
        PINNING.unpinCID(cid_OFFSET_PLACEOFBIRTH_CID);
        REMOVE.remove(cid_OFFSET_PLACEOFBIRTH_CID);
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
            final long cid_OFFSET_PLACEOFBIRTH_CID = RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_CID);
            PINNING.unpinCID(cid_OFFSET_PLACEOFBIRTH_CID);
            REMOVE.remove(cid_OFFSET_PLACEOFBIRTH_CID);
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
            PINNING.unpinCID(p_cids[i]);
            REMOVE.remove(p_cids[i]);
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

    public static short getAge(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readShort(addr, OFFSET_AGE);
    }

    public static short getAgeViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readShort(p_addr, OFFSET_AGE);
    }

    public static void setAge(final long p_cid, final short p_age) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeShort(addr, OFFSET_AGE, p_age);
    }

    public static void setAgeViaAddress(final long p_addr, final short p_age) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeShort(p_addr, OFFSET_AGE, p_age);
    }

    public static long getDateOfBirth(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_DATEOFBIRTH);
    }

    public static long getDateOfBirthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_DATEOFBIRTH);
    }

    public static void setDateOfBirth(final long p_cid, final long p_dateofbirth) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeLong(addr, OFFSET_DATEOFBIRTH, p_dateofbirth);
    }

    public static void setDateOfBirthViaAddress(final long p_addr, final long p_dateofbirth) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(p_addr, OFFSET_DATEOFBIRTH, p_dateofbirth);
    }

    public static long getPlaceOfBirthCityCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_CID);
    }

    public static long getPlaceOfBirthCityAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_PLACEOFBIRTH_ADDR);
    }

    public static long getPlaceOfBirthCityCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PLACEOFBIRTH_CID);
    }

    public static long getPlaceOfBirthCityAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_PLACEOFBIRTH_ADDR);
    }

    public static void setPlaceOfBirthCityCID(final long p_cid, final long p_placeofbirth_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_placeofbirth_cid);
        RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_CID, p_placeofbirth_cid);
        RAWWRITE.writeLong(addr, OFFSET_PLACEOFBIRTH_ADDR, addr2);
    }

    public static void setPlaceOfBirthCityCIDViaAddress(final long p_addr, final long p_placeofbirth_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_placeofbirth_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PLACEOFBIRTH_CID, p_placeofbirth_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_PLACEOFBIRTH_ADDR, addr2);
    }

    public static String getHomeAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_HOMEADDRESS_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_ADDR), 0, len));
    }

    public static String getHomeAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_HOMEADDRESS_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_HOMEADDRESS_ADDR), 0, len));
    }

    public static void setHomeAddress(final long p_cid, final String p_homeaddress) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_HOMEADDRESS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_HOMEADDRESS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_homeaddress == null || p_homeaddress.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_HOMEADDRESS_ADDR, 0);
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

    public static void setHomeAddressViaAddress(final long p_addr, final String p_homeaddress) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_HOMEADDRESS_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_HOMEADDRESS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_HOMEADDRESS_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_HOMEADDRESS_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_HOMEADDRESS_LENGTH, (p_homeaddress == null ? -1 : 0));
        }

        if (p_homeaddress == null || p_homeaddress.length() == 0) {
            return;
        }

        final byte[] str = p_homeaddress.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_HOMEADDRESS_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_HOMEADDRESS_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_HOMEADDRESS_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static String getEmail(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_EMAIL_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(addr, OFFSET_EMAIL_ADDR), 0, len));
    }

    public static String getEmailViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_EMAIL_LENGTH);

        if (len < 0) {
            return null;
        } else if (len == 0) {
            return "";
        }

        return new String(RAWREAD.readByteArray(RAWREAD.readLong(p_addr, OFFSET_EMAIL_ADDR), 0, len));
    }

    public static void setEmail(final long p_cid, final String p_email) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_EMAIL_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_EMAIL_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
        }

        if (p_email == null || p_email.length() == 0) {
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_EMAIL_ADDR, 0);
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

    public static void setEmailViaAddress(final long p_addr, final String p_email) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_EMAIL_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_EMAIL_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_EMAIL_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_EMAIL_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_EMAIL_LENGTH, (p_email == null ? -1 : 0));
        }

        if (p_email == null || p_email.length() == 0) {
            return;
        }

        final byte[] str = p_email.getBytes();
        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, str.length);
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_EMAIL_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_EMAIL_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_EMAIL_LENGTH, str.length);
        RAWWRITE.writeByteArray(addr2, 0, str);
    }

    public static int getFavoriteNumbersLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);
    }

    public static int getFavoriteNumbersLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH);
    }

    public static int getFavoriteNumbers(final long p_cid, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    public static int getFavoriteNumbersViaAddress(final long p_addr, final int index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_FAVORITENUMBERS_ADDR);
        return RAWREAD.readInt(addr2, (4 * index));
    }

    public static int[] getFavoriteNumbers(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR), 0, len);
    }

    public static int[] getFavoriteNumbersViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readIntArray(RAWREAD.readLong(p_addr, OFFSET_FAVORITENUMBERS_ADDR), 0, len);
    }

    public static void setFavoriteNumbers(final long p_cid, final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    public static void setFavoriteNumbersViaAddress(final long p_addr, final int index, final int value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH);

        if (index < 0 || index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_FAVORITENUMBERS_ADDR);
        RAWWRITE.writeInt(addr2, (4 * index), value);
    }

    public static void setFavoriteNumbers(final long p_cid, final int[] p_favoritenumbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAVORITENUMBERS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FAVORITENUMBERS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
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

    public static void setFavoriteNumbersViaAddress(final long p_addr, final int[] p_favoritenumbers) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_FAVORITENUMBERS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_FAVORITENUMBERS_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH, 0);
        }

        if (p_favoritenumbers == null || p_favoritenumbers.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (4 * p_favoritenumbers.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_FAVORITENUMBERS_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_FAVORITENUMBERS_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_FAVORITENUMBERS_LENGTH, p_favoritenumbers.length);
        RAWWRITE.writeIntArray(addr2, 0, p_favoritenumbers);
    }

    public static int getFriendsPersonLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);
    }

    public static long getFriendsPersonCID(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), (8 * p_index));
    }

    public static long getFriendsPersonAddress(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), (8 * (len + p_index)));
    }

    public static long[] getFriendsPersonCIDs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), 0, len);
    }

    public static long[] getFriendsPersonAddresses(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR), (8 * len), len);
    }

    public static int getFriendsPersonLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);
    }

    public static long getFriendsPersonCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_FRIENDS_ADDR), (8 * p_index));
    }

    public static long getFriendsPersonAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_FRIENDS_ADDR), (8 * (len + p_index)));
    }

    public static long[] getFriendsPersonCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_FRIENDS_ADDR), 0, len);
    }

    public static long[] getFriendsPersonAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_FRIENDS_ADDR), (8 * len), len);
    }

    public static void setFriendsPersonCID(final long p_cid, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FRIENDS_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setFriendsPersonCIDs(final long p_cid, final long[] p_friends) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FRIENDS_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FRIENDS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, 0);
        }

        if (p_friends == null || p_friends.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_friends.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_FRIENDS_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_FRIENDS_LENGTH, p_friends.length);
        RAWWRITE.writeLongArray(addr2, 0, p_friends);
        for (int i = 0; i < p_friends.length; i ++) {
            final long addr3 = PINNING.translate(p_friends[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_friends.length + i)), addr3);
        }
    }

    public static void setFriendsPersonCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_FRIENDS_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setFriendsPersonCIDsViaAddress(final long p_addr, final long[] p_friends) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FRIENDS_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_FRIENDS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_FRIENDS_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_FRIENDS_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_FRIENDS_LENGTH, 0);
        }

        if (p_friends == null || p_friends.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_friends.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_FRIENDS_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_FRIENDS_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_FRIENDS_LENGTH, p_friends.length);
        RAWWRITE.writeLongArray(addr2, 0, p_friends);
        for (int i = 0; i < p_friends.length; i ++) {
            final long addr3 = PINNING.translate(p_friends[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_friends.length + i)), addr3);
        }
    }

    public static int getFamilyPersonLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);
    }

    public static long getFamilyPersonCID(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), (8 * p_index));
    }

    public static long getFamilyPersonAddress(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), (8 * (len + p_index)));
    }

    public static long[] getFamilyPersonCIDs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), 0, len);
    }

    public static long[] getFamilyPersonAddresses(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR), (8 * len), len);
    }

    public static int getFamilyPersonLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);
    }

    public static long getFamilyPersonCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_FAMILY_ADDR), (8 * p_index));
    }

    public static long getFamilyPersonAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_FAMILY_ADDR), (8 * (len + p_index)));
    }

    public static long[] getFamilyPersonCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_FAMILY_ADDR), 0, len);
    }

    public static long[] getFamilyPersonAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_FAMILY_ADDR), (8 * len), len);
    }

    public static void setFamilyPersonCID(final long p_cid, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_FAMILY_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setFamilyPersonCIDs(final long p_cid, final long[] p_family) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_FAMILY_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_FAMILY_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, 0);
        }

        if (p_family == null || p_family.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_family.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_FAMILY_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_FAMILY_LENGTH, p_family.length);
        RAWWRITE.writeLongArray(addr2, 0, p_family);
        for (int i = 0; i < p_family.length; i ++) {
            final long addr3 = PINNING.translate(p_family[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_family.length + i)), addr3);
        }
    }

    public static void setFamilyPersonCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_FAMILY_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setFamilyPersonCIDsViaAddress(final long p_addr, final long[] p_family) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_FAMILY_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_FAMILY_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_FAMILY_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_FAMILY_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_FAMILY_LENGTH, 0);
        }

        if (p_family == null || p_family.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_family.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_FAMILY_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_FAMILY_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_FAMILY_LENGTH, p_family.length);
        RAWWRITE.writeLongArray(addr2, 0, p_family);
        for (int i = 0; i < p_family.length; i ++) {
            final long addr3 = PINNING.translate(p_family[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_family.length + i)), addr3);
        }
    }

    private DirectPerson() {}

    public static DirectPerson use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        DirectPerson tmp = new DirectPerson();
        tmp.m_addr = PINNING.translate(p_cid);
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

    public long getPlaceOfBirthCityCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PLACEOFBIRTH_CID);
    }

    public long getPlaceOfBirthCityAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_PLACEOFBIRTH_ADDR);
    }

    public void setPlaceOfBirthCityCID(final long p_placeofbirth_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_placeofbirth_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PLACEOFBIRTH_CID, p_placeofbirth_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_PLACEOFBIRTH_ADDR, addr2);
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

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_homeaddress == null || p_homeaddress.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_HOMEADDRESS_ADDR, 0);
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

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
        }

        if (p_email == null || p_email.length() == 0) {
            RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_EMAIL_ADDR, 0);
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

        if (cid != -1) {
            PINNING.unpinCID(cid);
            REMOVE.remove(cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_FAVORITENUMBERS_ADDR, 0);
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

    public long getFriendsPersonCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), (8 * p_index));
    }

    public long getFriendsPersonAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), (8 * (len + p_index)));
    }

    public long[] getFriendsPersonCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), 0, len);
    }

    public long[] getFriendsPersonAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR), (8 * len), len);
    }

    public void setFriendsPersonCID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_FRIENDS_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public void setFriendsPersonCIDs(final long[] p_friends) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FRIENDS_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_FRIENDS_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_FRIENDS_ADDR, 0);
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
        RAWWRITE.writeLongArray(addr2, 0, p_friends);
        for (int i = 0; i < p_friends.length; i ++) {
            final long addr3 = PINNING.translate(p_friends[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_friends.length + i)), addr3);
        }
    }

    public int getFamilyPersonLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);
    }

    public long getFamilyPersonCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), (8 * p_index));
    }

    public long getFamilyPersonAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), (8 * (len + p_index)));
    }

    public long[] getFamilyPersonCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), 0, len);
    }

    public long[] getFamilyPersonAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR), (8 * len), len);
    }

    public void setFamilyPersonCID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_FAMILY_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public void setFamilyPersonCIDs(final long[] p_family) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_FAMILY_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_FAMILY_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_FAMILY_ADDR, 0);
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
        RAWWRITE.writeLongArray(addr2, 0, p_family);
        for (int i = 0; i < p_family.length; i ++) {
            final long addr3 = PINNING.translate(p_family[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_family.length + i)), addr3);
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
