package de.hhu.bsinfo.dxram.dxddl;

import de.hhu.bsinfo.dxram.chunk.operation.CreateLocal;
import de.hhu.bsinfo.dxram.chunk.operation.CreateReservedLocal;
import de.hhu.bsinfo.dxram.chunk.operation.ReserveLocal;
import de.hhu.bsinfo.dxram.chunk.operation.Remove;
import de.hhu.bsinfo.dxram.chunk.operation.PinningLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawReadLocal;
import de.hhu.bsinfo.dxram.chunk.operation.RawWriteLocal;

public final class DirectCountry implements AutoCloseable {

    private static final int OFFSET_NAME_LENGTH = 0;
    private static final int OFFSET_NAME_CID = 4;
    private static final int OFFSET_NAME_ADDR = 12;
    private static final int OFFSET_POPULATION = 20;
    private static final int OFFSET_AREA = 28;
    private static final int OFFSET_CAPITAL_CID = 32;
    private static final int OFFSET_CAPITAL_ADDR = 40;
    private static final int OFFSET_CITIES_LENGTH = 48;
    private static final int OFFSET_CITIES_CID = 52;
    private static final int OFFSET_CITIES_ADDR = 60;
    private static final int SIZE = 68;
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
        RAWWRITE.writeLong(addr, OFFSET_CAPITAL_CID, -1);
        RAWWRITE.writeLong(addr, OFFSET_CITIES_CID, -1);
        RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
        RAWWRITE.writeInt(addr, OFFSET_CITIES_LENGTH, 0);
        RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_CAPITAL_ADDR, 0);
        RAWWRITE.writeLong(addr, OFFSET_CITIES_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_CAPITAL_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_CITIES_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CAPITAL_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_ADDR, 0);
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
            RAWWRITE.writeLong(addr, OFFSET_CAPITAL_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_CID, -1);
            RAWWRITE.writeInt(addr, OFFSET_NAME_LENGTH, -1);
            RAWWRITE.writeInt(addr, OFFSET_CITIES_LENGTH, 0);
            RAWWRITE.writeLong(addr, OFFSET_NAME_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CAPITAL_ADDR, 0);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_ADDR, 0);
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
        final long cid_OFFSET_CAPITAL_CID = RAWREAD.readLong(addr, OFFSET_CAPITAL_CID);
        PINNING.unpinCID(cid_OFFSET_CAPITAL_CID);
        REMOVE.remove(cid_OFFSET_CAPITAL_CID);
        final long cid_OFFSET_CITIES_CID = RAWREAD.readLong(addr, OFFSET_CITIES_CID);
        PINNING.unpinCID(cid_OFFSET_CITIES_CID);
        REMOVE.remove(cid_OFFSET_CITIES_CID);
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
            final long cid_OFFSET_CAPITAL_CID = RAWREAD.readLong(addr, OFFSET_CAPITAL_CID);
            PINNING.unpinCID(cid_OFFSET_CAPITAL_CID);
            REMOVE.remove(cid_OFFSET_CAPITAL_CID);
            final long cid_OFFSET_CITIES_CID = RAWREAD.readLong(addr, OFFSET_CITIES_CID);
            PINNING.unpinCID(cid_OFFSET_CITIES_CID);
            REMOVE.remove(cid_OFFSET_CITIES_CID);
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

    public static long getPopulation(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_POPULATION);
    }

    public static long getPopulationViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_POPULATION);
    }

    public static void setPopulation(final long p_cid, final long p_population) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeLong(addr, OFFSET_POPULATION, p_population);
    }

    public static void setPopulationViaAddress(final long p_addr, final long p_population) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(p_addr, OFFSET_POPULATION, p_population);
    }

    public static int getArea(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_AREA);
    }

    public static int getAreaViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_AREA);
    }

    public static void setArea(final long p_cid, final int p_area) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        RAWWRITE.writeInt(addr, OFFSET_AREA, p_area);
    }

    public static void setAreaViaAddress(final long p_addr, final int p_area) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(p_addr, OFFSET_AREA, p_area);
    }

    public static long getCapitalCityCID(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_CAPITAL_CID);
    }

    public static long getCapitalCityAddress(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readLong(addr, OFFSET_CAPITAL_ADDR);
    }

    public static long getCapitalCityCIDViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_CAPITAL_CID);
    }

    public static long getCapitalCityAddressViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(p_addr, OFFSET_CAPITAL_ADDR);
    }

    public static void setCapitalCityCID(final long p_cid, final long p_capital_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final long addr2 = PINNING.translate(p_capital_cid);
        RAWWRITE.writeLong(addr, OFFSET_CAPITAL_CID, p_capital_cid);
        RAWWRITE.writeLong(addr, OFFSET_CAPITAL_ADDR, addr2);
    }

    public static void setCapitalCityCIDViaAddress(final long p_addr, final long p_capital_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_capital_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_CAPITAL_CID, p_capital_cid);
        RAWWRITE.writeLong(p_addr, OFFSET_CAPITAL_ADDR, addr2);
    }

    public static int getCitiesCityLength(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        return RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);
    }

    public static long getCitiesCityCID(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_CITIES_ADDR), (8 * p_index));
    }

    public static long getCitiesCityAddress(final long p_cid, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(addr, OFFSET_CITIES_ADDR), (8 * (len + p_index)));
    }

    public static long[] getCitiesCityCIDs(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CITIES_ADDR), 0, len);
    }

    public static long[] getCitiesCityAddresses(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(addr, OFFSET_CITIES_ADDR), (8 * len), len);
    }

    public static int getCitiesCityLengthViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);
    }

    public static long getCitiesCityCIDViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CITIES_ADDR), (8 * p_index));
    }

    public static long getCitiesCityAddressViaAddress(final long p_addr, final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(p_addr, OFFSET_CITIES_ADDR), (8 * (len + p_index)));
    }

    public static long[] getCitiesCityCIDsViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CITIES_ADDR), 0, len);
    }

    public static long[] getCitiesCityAddressesViaAddress(final long p_addr) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(p_addr, OFFSET_CITIES_ADDR), (8 * len), len);
    }

    public static void setCitiesCityCID(final long p_cid, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(addr, OFFSET_CITIES_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setCitiesCityCIDs(final long p_cid, final long[] p_cities) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr = PINNING.translate(p_cid);
        final int len = RAWREAD.readInt(addr, OFFSET_CITIES_LENGTH);
        final long array_cid = RAWREAD.readLong(addr, OFFSET_CITIES_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_CID, -1);
            RAWWRITE.writeLong(addr, OFFSET_CITIES_ADDR, 0);
            RAWWRITE.writeInt(addr, OFFSET_CITIES_LENGTH, 0);
        }

        if (p_cities == null || p_cities.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_cities.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(addr, OFFSET_CITIES_CID, new_cid[0]);
        RAWWRITE.writeLong(addr, OFFSET_CITIES_ADDR, addr2);
        RAWWRITE.writeInt(addr, OFFSET_CITIES_LENGTH, p_cities.length);
        RAWWRITE.writeLongArray(addr2, 0, p_cities);
        for (int i = 0; i < p_cities.length; i ++) {
            final long addr3 = PINNING.translate(p_cities[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_cities.length + i)), addr3);
        }
    }

    public static void setCitiesCityCIDViaAddress(final long p_addr, final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(p_addr, OFFSET_CITIES_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public static void setCitiesCityCIDsViaAddress(final long p_addr, final long[] p_cities) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(p_addr, OFFSET_CITIES_LENGTH);
        final long array_cid = RAWREAD.readLong(p_addr, OFFSET_CITIES_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(p_addr, OFFSET_CITIES_CID, -1);
            RAWWRITE.writeLong(p_addr, OFFSET_CITIES_ADDR, 0);
            RAWWRITE.writeInt(p_addr, OFFSET_CITIES_LENGTH, 0);
        }

        if (p_cities == null || p_cities.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_cities.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(p_addr, OFFSET_CITIES_CID, new_cid[0]);
        RAWWRITE.writeLong(p_addr, OFFSET_CITIES_ADDR, addr2);
        RAWWRITE.writeInt(p_addr, OFFSET_CITIES_LENGTH, p_cities.length);
        RAWWRITE.writeLongArray(addr2, 0, p_cities);
        for (int i = 0; i < p_cities.length; i ++) {
            final long addr3 = PINNING.translate(p_cities[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_cities.length + i)), addr3);
        }
    }

    private DirectCountry() {}

    public static DirectCountry use(final long p_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        DirectCountry tmp = new DirectCountry();
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

    public long getPopulation() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_POPULATION);
    }

    public void setPopulation(final long p_population) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeLong(m_addr, OFFSET_POPULATION, p_population);
    }

    public int getArea() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_AREA);
    }

    public void setArea(final int p_area) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        RAWWRITE.writeInt(m_addr, OFFSET_AREA, p_area);
    }

    public long getCapitalCityCID() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_CAPITAL_CID);
    }

    public long getCapitalCityAddress() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readLong(m_addr, OFFSET_CAPITAL_ADDR);
    }

    public void setCapitalCityCID(final long p_capital_cid) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final long addr2 = PINNING.translate(p_capital_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_CAPITAL_CID, p_capital_cid);
        RAWWRITE.writeLong(m_addr, OFFSET_CAPITAL_ADDR, addr2);
    }

    public int getCitiesCityLength() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        return RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);
    }

    public long getCitiesCityCID(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CITIES_ADDR), (8 * p_index));
    }

    public long getCitiesCityAddress(final int p_index) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        return RAWREAD.readLong(RAWREAD.readLong(m_addr, OFFSET_CITIES_ADDR), (8 * (len + p_index)));
    }

    public long[] getCitiesCityCIDs() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CITIES_ADDR), 0, len);
    }

    public long[] getCitiesCityAddresses() {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);

        if (len <= 0) {
            return null;
        }

        return RAWREAD.readLongArray(RAWREAD.readLong(m_addr, OFFSET_CITIES_ADDR), (8 * len), len);
    }

    public void setCitiesCityCID(final int p_index, final long p_value) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);

        if (p_index < 0 || p_index >= len) {
            throw new ArrayIndexOutOfBoundsException();
        }

        final long addr2 = RAWREAD.readLong(m_addr, OFFSET_CITIES_ADDR);
        RAWWRITE.writeLong(addr2, (8 * p_index), p_value);
        final long addr3 = PINNING.translate(p_value);
        RAWWRITE.writeLong(addr2, (8 * (len + p_index)), addr3);
    }

    public void setCitiesCityCIDs(final long[] p_cities) {
        if (!INITIALIZED) {
            throw new RuntimeException("Not initialized!");
        }

        final int len = RAWREAD.readInt(m_addr, OFFSET_CITIES_LENGTH);
        final long array_cid = RAWREAD.readLong(m_addr, OFFSET_CITIES_CID);

        if (array_cid != -1) {
            PINNING.unpinCID(array_cid);
            REMOVE.remove(array_cid);
            RAWWRITE.writeLong(m_addr, OFFSET_CITIES_CID, -1);
            RAWWRITE.writeLong(m_addr, OFFSET_CITIES_ADDR, 0);
            RAWWRITE.writeInt(m_addr, OFFSET_CITIES_LENGTH, 0);
        }

        if (p_cities == null || p_cities.length == 0) {
            return;
        }

        final long[] new_cid = new long[1];
        CREATE.create(new_cid, 1, (16 * p_cities.length));
        final long addr2 = PINNING.pin(new_cid[0]).getAddress();
        RAWWRITE.writeLong(m_addr, OFFSET_CITIES_CID, new_cid[0]);
        RAWWRITE.writeLong(m_addr, OFFSET_CITIES_ADDR, addr2);
        RAWWRITE.writeInt(m_addr, OFFSET_CITIES_LENGTH, p_cities.length);
        RAWWRITE.writeLongArray(addr2, 0, p_cities);
        for (int i = 0; i < p_cities.length; i ++) {
            final long addr3 = PINNING.translate(p_cities[i]);
            RAWWRITE.writeLong(addr2, (8 * (p_cities.length + i)), addr3);
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
