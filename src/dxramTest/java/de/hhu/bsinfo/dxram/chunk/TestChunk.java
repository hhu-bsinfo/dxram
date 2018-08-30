package de.hhu.bsinfo.dxram.chunk;

import java.util.Arrays;

import org.junit.Assert;

import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class TestChunk extends DataStructure {
    private static final byte DATA_V1 = 0x12;
    private static final short DATA_V2 = (short) 0xABCD;
    private static final char DATA_V3 = 'X';
    private static final int DATA_V4 = 0x12345678;
    private static final long DATA_V5 = 0xDEADBEEFCA4EL;
    private static final String DATA_STR = "0-kh2ywe6yfsdhsfga";

    private static final int ARR1_SIZE = 1233;
    private static final int ARR2_SIZE = 912;
    private static final int ARR3_SIZE = 21;
    private static final int ARR4_SIZE = 555;
    private static final int ARR5_SIZE = 999;

    private byte m_v1;
    private short m_v2;
    private char m_v3;
    private int m_v4;
    private long m_v5;

    private String m_str;

    private byte[] m_arr1;
    private short[] m_arr2;
    private char[] m_arr3;
    private int[] m_arr4;
    private long[] m_arr5;

    public TestChunk(final boolean p_dummy) {
        m_v1 = DATA_V1;
        m_v2 = DATA_V2;
        m_v3 = DATA_V3;
        m_v4 = DATA_V4;
        m_v5 = DATA_V5;

        m_str = DATA_STR;

        m_arr1 = new byte[ARR1_SIZE];
        Arrays.fill(m_arr1, DATA_V1);

        m_arr2 = new short[ARR2_SIZE];
        Arrays.fill(m_arr2, DATA_V2);

        m_arr3 = new char[ARR3_SIZE];
        Arrays.fill(m_arr3, DATA_V3);

        m_arr4 = new int[ARR4_SIZE];
        Arrays.fill(m_arr4, DATA_V4);

        m_arr5 = new long[ARR5_SIZE];
        Arrays.fill(m_arr5, DATA_V5);
    }

    public void clear() {
        m_v1 = 0;
        m_v2 = 0;
        m_v3 = 0;
        m_v4 = 0;
        m_v5 = 0;

        m_str = "";

        m_arr1 = null;
        m_arr2 = null;
        m_arr3 = null;
        m_arr4 = null;
        m_arr5 = null;
    }

    public void verifyContents() {
        Assert.assertEquals(DATA_V1, m_v1);
        Assert.assertEquals(DATA_V2, m_v2);
        Assert.assertEquals(DATA_V3, m_v3);
        Assert.assertEquals(DATA_V4, m_v4);
        Assert.assertEquals(DATA_V5, m_v5);
        Assert.assertEquals(DATA_STR, m_str);

        for (int i = 0; i < m_arr1.length; i++) {
            Assert.assertEquals(DATA_V1, m_arr1[i]);
        }

        for (int i = 0; i < m_arr2.length; i++) {
            Assert.assertEquals(DATA_V2, m_arr2[i]);
        }

        for (int i = 0; i < m_arr3.length; i++) {
            Assert.assertEquals(DATA_V3, m_arr3[i]);
        }

        for (int i = 0; i < m_arr4.length; i++) {
            Assert.assertEquals(DATA_V4, m_arr4[i]);
        }

        for (int i = 0; i < m_arr5.length; i++) {
            Assert.assertEquals(DATA_V5, m_arr5[i]);
        }
    }

    public byte getV1() {
        return m_v1;
    }

    public short getV2() {
        return m_v2;
    }

    public char getV3() {
        return m_v3;
    }

    public int getV4() {
        return m_v4;
    }

    public long getV5() {
        return m_v5;
    }

    public String getStr() {
        return m_str;
    }

    public byte[] getArr1() {
        return m_arr1;
    }

    public short[] getArr2() {
        return m_arr2;
    }

    public char[] getArr3() {
        return m_arr3;
    }

    public int[] getArr4() {
        return m_arr4;
    }

    public long[] getArr5() {
        return m_arr5;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeByte(m_v1);
        p_exporter.writeShort(m_v2);
        p_exporter.writeChar(m_v3);
        p_exporter.writeInt(m_v4);
        p_exporter.writeLong(m_v5);
        p_exporter.writeString(m_str);
        p_exporter.writeByteArray(m_arr1);
        p_exporter.writeShortArray(m_arr2);
        p_exporter.writeCharArray(m_arr3);
        p_exporter.writeIntArray(m_arr4);
        p_exporter.writeLongArray(m_arr5);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_v1 = p_importer.readByte(m_v1);
        m_v2 = p_importer.readShort(m_v2);
        m_v3 = p_importer.readChar(m_v3);
        m_v4 = p_importer.readInt(m_v4);
        m_v5 = p_importer.readLong(m_v5);
        m_str = p_importer.readString(m_str);
        m_arr1 = p_importer.readByteArray(m_arr1);
        m_arr2 = p_importer.readShortArray(m_arr2);
        m_arr3 = p_importer.readCharArray(m_arr3);
        m_arr4 = p_importer.readIntArray(m_arr4);
        m_arr5 = p_importer.readLongArray(m_arr5);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + Short.BYTES + Character.BYTES + Integer.BYTES + Long.BYTES +
                ObjectSizeUtil.sizeofString(m_str) + ObjectSizeUtil.sizeofByteArray(m_arr1) +
                ObjectSizeUtil.sizeofShortArray(m_arr2) + ObjectSizeUtil.sizeofCharArray(m_arr3) +
                ObjectSizeUtil.sizeofIntArray(m_arr4) + ObjectSizeUtil.sizeofLongArray(m_arr5);
    }
}
