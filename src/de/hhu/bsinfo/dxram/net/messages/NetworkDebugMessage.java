/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.net.messages;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.Message;
import de.hhu.bsinfo.utils.serialization.ObjectSizeUtil;

/**
 * Network message to debug the network subsystem by trying to trigger as many code paths as possible when sending messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.08.17
 */
public class NetworkDebugMessage extends Message {
    private static final int PRIMITIVE_COUNT = 20;
    private static final int ARRAY_LENGTH = 20;

    private static final boolean VAL_BOOL = false;
    private static final byte VAL_BYTE = (byte) 0xAA;
    private static final short VAL_SHORT = (short) 0xBBCC;
    private static final int VAL_INT = 0xDDEEFF99;
    private static final long VAL_LONG = 0x1122334455667788L;
    private static final float VAL_FLOAT = 12345.1234f;
    private static final double VAL_DOUBLE = 987654.987654;
    private static final int VAL_COMP_NUM_1 = 0x1F;
    private static final int VAL_COMP_NUM_2 = 0x1FEC;
    private static final int VAL_COMP_NUM_3 = 0x1FECDD;
    private static final String VAL_STR = "abcdefghijklmnopqrstuvwxyz1234567890";

    private static final byte[] VAL_BYTE_ARR = new byte[ARRAY_LENGTH];
    private static final short[] VAL_SHORT_ARR = new short[ARRAY_LENGTH];
    private static final int[] VAL_INT_ARR = new int[ARRAY_LENGTH];
    private static final long[] VAL_LONG_ARR = new long[ARRAY_LENGTH];

    static {
        for (int i = 0; i < ARRAY_LENGTH; i++) {
            VAL_BYTE_ARR[i] = (byte) i;
            VAL_SHORT_ARR[i] = (short) i;
            VAL_INT_ARR[i] = i;
            VAL_LONG_ARR[i] = i;
        }
    }

    private boolean[] m_valBool = new boolean[PRIMITIVE_COUNT];
    private byte[] m_valByte = new byte[PRIMITIVE_COUNT];
    private short[] m_valShort = new short[PRIMITIVE_COUNT];
    private int[] m_valInt = new int[PRIMITIVE_COUNT];
    private long[] m_valLong = new long[PRIMITIVE_COUNT];
    private float[] m_valFloat = new float[PRIMITIVE_COUNT];
    private double[] m_valDouble = new double[PRIMITIVE_COUNT];
    private int[] m_compNum1 = new int[PRIMITIVE_COUNT];
    private int[] m_compNum2 = new int[PRIMITIVE_COUNT];
    private int[] m_compNum3 = new int[PRIMITIVE_COUNT];
    private String m_valStr = "";

    private byte[][] m_valByteArr = new byte[4][ARRAY_LENGTH];
    private short[][] m_valShortArr = new short[4][ARRAY_LENGTH];
    private int[][] m_valIntArr = new int[4][ARRAY_LENGTH];
    private long[][] m_valLongArr = new long[4][ARRAY_LENGTH];
    private String[] m_valStrArr = new String[ARRAY_LENGTH];

    /**
     * Creates an instance of NetworkDebugMessage.
     * This constructor is used when receiving this message.
     */
    public NetworkDebugMessage() {
        super();

        for (int i = 0; i < m_valStrArr.length; i++) {
            m_valStrArr[i] = "";
        }
    }

    /**
     * Creates an instance of NetworkDebugMessage.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *         the destination node id.
     */
    public NetworkDebugMessage(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.NETWORK_MESSAGES_TYPE, NetworkMessages.SUBTYPE_DEBUG_MESSAGE);

        for (int i = 0; i < PRIMITIVE_COUNT; i++) {
            m_valBool[i] = VAL_BOOL;
            m_valByte[i] = VAL_BYTE;
            m_valShort[i] = VAL_SHORT;
            m_valInt[i] = VAL_INT;
            m_valLong[i] = VAL_LONG;
            m_valFloat[i] = VAL_FLOAT;
            m_valDouble[i] = VAL_DOUBLE;
            m_compNum1[i] = VAL_COMP_NUM_1;
            m_compNum2[i] = VAL_COMP_NUM_2;
            m_compNum3[i] = VAL_COMP_NUM_3;
        }

        m_valStr = VAL_STR;

        for (int i = 0; i < 4; i++) {
            m_valByteArr[i] = VAL_BYTE_ARR;
            m_valShortArr[i] = VAL_SHORT_ARR;
            m_valIntArr[i] = VAL_INT_ARR;
            m_valLongArr[i] = VAL_LONG_ARR;
        }
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        for (int i = 0; i < PRIMITIVE_COUNT; i++) {
            p_exporter.writeBoolean(m_valBool[i]);
            p_exporter.writeByte(m_valByte[i]);
            p_exporter.writeShort(m_valShort[i]);
            p_exporter.writeInt(m_valInt[i]);
            p_exporter.writeLong(m_valLong[i]);
            p_exporter.writeFloat(m_valFloat[i]);
            p_exporter.writeDouble(m_valDouble[i]);
            p_exporter.writeCompactNumber(m_compNum1[i]);
            p_exporter.writeCompactNumber(m_compNum2[i]);
            p_exporter.writeCompactNumber(m_compNum3[i]);
        }

        p_exporter.writeString(m_valStr);

        p_exporter.writeBytes(m_valByteArr[0]);
        p_exporter.writeShorts(m_valShortArr[0]);
        p_exporter.writeInts(m_valIntArr[0]);
        p_exporter.writeLongs(m_valLongArr[0]);

        p_exporter.writeBytes(m_valByteArr[1]);
        p_exporter.writeShorts(m_valShortArr[1]);
        p_exporter.writeInts(m_valIntArr[1]);
        p_exporter.writeLongs(m_valLongArr[1]);

        p_exporter.writeByteArray(m_valByteArr[2]);
        p_exporter.writeShortArray(m_valShortArr[2]);
        p_exporter.writeIntArray(m_valIntArr[2]);
        p_exporter.writeLongArray(m_valLongArr[2]);

        p_exporter.writeBytes(m_valByteArr[3], m_valByteArr[3].length / 2, m_valByteArr[3].length / 4);
        p_exporter.writeShorts(m_valShortArr[3], m_valShortArr[3].length / 2, m_valShortArr[3].length / 4);
        p_exporter.writeInts(m_valIntArr[3], m_valIntArr[3].length / 2, m_valIntArr[3].length / 4);
        p_exporter.writeLongs(m_valLongArr[3], m_valLongArr[3].length / 2, m_valLongArr[3].length / 4);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        for (int i = 0; i < PRIMITIVE_COUNT; i++) {
            m_valBool[i] = p_importer.readBoolean(m_valBool[i]);
            m_valByte[i] = p_importer.readByte(m_valByte[i]);
            m_valShort[i] = p_importer.readShort(m_valShort[i]);
            m_valInt[i] = p_importer.readInt(m_valInt[i]);
            m_valLong[i] = p_importer.readLong(m_valLong[i]);
            m_valFloat[i] = p_importer.readFloat(m_valFloat[i]);
            m_valDouble[i] = p_importer.readDouble(m_valDouble[i]);
            m_compNum1[i] = p_importer.readCompactNumber(m_compNum1[i]);
            m_compNum2[i] = p_importer.readCompactNumber(m_compNum2[i]);
            m_compNum3[i] = p_importer.readCompactNumber(m_compNum3[i]);
        }

        m_valStr = p_importer.readString(m_valStr);

        p_importer.readBytes(m_valByteArr[0]);
        p_importer.readShorts(m_valShortArr[0]);
        p_importer.readInts(m_valIntArr[0]);
        p_importer.readLongs(m_valLongArr[0]);

        p_importer.readBytes(m_valByteArr[1]);
        p_importer.readShorts(m_valShortArr[1]);
        p_importer.readInts(m_valIntArr[1]);
        p_importer.readLongs(m_valLongArr[1]);

        m_valByteArr[2] = p_importer.readByteArray(m_valByteArr[2]);
        m_valShortArr[2] = p_importer.readShortArray(m_valShortArr[2]);
        m_valIntArr[2] = p_importer.readIntArray(m_valIntArr[2]);
        m_valLongArr[2] = p_importer.readLongArray(m_valLongArr[2]);

        p_importer.readBytes(m_valByteArr[3], m_valByteArr[3].length / 2, m_valByteArr[3].length / 4);
        p_importer.readShorts(m_valShortArr[3], m_valShortArr[3].length / 2, m_valShortArr[3].length / 4);
        p_importer.readInts(m_valIntArr[3], m_valIntArr[3].length / 2, m_valIntArr[3].length / 4);
        p_importer.readLongs(m_valLongArr[3], m_valLongArr[3].length / 2, m_valLongArr[3].length / 4);

        verify();
    }

    @Override
    protected final int getPayloadLength() {
        int len = 0;

        for (int i = 0; i < PRIMITIVE_COUNT; i++) {
            len += ObjectSizeUtil.sizeofBoolean();
            len += Byte.BYTES;
            len += Short.BYTES;
            len += Integer.BYTES;
            len += Long.BYTES;
            len += Float.BYTES;
            len += Double.BYTES;
            len += ObjectSizeUtil.sizeofCompactedNumber(VAL_COMP_NUM_1);
            len += ObjectSizeUtil.sizeofCompactedNumber(VAL_COMP_NUM_2);
            len += ObjectSizeUtil.sizeofCompactedNumber(VAL_COMP_NUM_3);
        }

        len += ObjectSizeUtil.sizeofString(VAL_STR);

        len += VAL_BYTE_ARR.length * Byte.BYTES;
        len += VAL_SHORT_ARR.length * Short.BYTES;
        len += VAL_INT_ARR.length * Integer.BYTES;
        len += VAL_LONG_ARR.length * Long.BYTES;

        len += VAL_BYTE_ARR.length * Byte.BYTES;
        len += VAL_SHORT_ARR.length * Short.BYTES;
        len += VAL_INT_ARR.length * Integer.BYTES;
        len += VAL_LONG_ARR.length * Long.BYTES;

        len += ObjectSizeUtil.sizeofByteArray(VAL_BYTE_ARR);
        len += ObjectSizeUtil.sizeofShortArray(VAL_SHORT_ARR);
        len += ObjectSizeUtil.sizeofIntArray(VAL_INT_ARR);
        len += ObjectSizeUtil.sizeofLongArray(VAL_LONG_ARR);

        len += VAL_BYTE_ARR.length / 4 * Byte.BYTES;
        len += VAL_SHORT_ARR.length / 4 * Short.BYTES;
        len += VAL_INT_ARR.length / 4 * Integer.BYTES;
        len += VAL_LONG_ARR.length / 4 * Long.BYTES;

        return len;
    }

    private void verify() {
        for (int i = 0; i < PRIMITIVE_COUNT; i++) {
            if (m_valBool[i] != VAL_BOOL) {
                throwVerificationException(0, i, 0);
            }

            if (m_valByte[i] != VAL_BYTE) {
                throwVerificationException(1, i, 0);
            }

            if (m_valShort[i] != VAL_SHORT) {
                throwVerificationException(2, i, 0);
            }

            if (m_valInt[i] != VAL_INT) {
                throwVerificationException(3, i, 0);
            }

            if (m_valLong[i] != VAL_LONG) {
                throwVerificationException(4, i, 0);
            }

            if (Math.abs(m_valFloat[i] - VAL_FLOAT) > 0.001f) {
                throwVerificationException(5, i, 0);
            }

            if (Math.abs(m_valDouble[i] - VAL_DOUBLE) > 0.001f) {
                throwVerificationException(6, i, 0);
            }

            if (m_compNum1[i] != VAL_COMP_NUM_1) {
                throwVerificationException(7, i, 0);
            }

            if (m_compNum2[i] != VAL_COMP_NUM_2) {
                throwVerificationException(8, i, 0);
            }

            if (m_compNum3[i] != VAL_COMP_NUM_3) {
                throwVerificationException(9, i, 0);
            }
        }

        if (!m_valStr.equals(VAL_STR)) {
            throwVerificationException(10, 0, 0);
        }

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < ARRAY_LENGTH; j++) {
                if (m_valByteArr[i][j] != VAL_BYTE_ARR[j]) {
                    throwVerificationException(11, i, j);
                }

                if (m_valShortArr[i][j] != VAL_SHORT_ARR[j]) {
                    throwVerificationException(12, i, j);
                }

                if (m_valIntArr[i][j] != VAL_INT_ARR[j]) {
                    throwVerificationException(13, i, j);
                }

                if (m_valLongArr[i][j] != VAL_LONG_ARR[j]) {
                    throwVerificationException(14, i, j);
                }
            }
        }

        for (int j = ARRAY_LENGTH / 2; j < ARRAY_LENGTH / 2 + ARRAY_LENGTH / 4; j++) {
            if (m_valByteArr[3][j] != VAL_BYTE_ARR[j]) {
                throwVerificationException(15, j, 0);
            }

            if (m_valShortArr[3][j] != VAL_SHORT_ARR[j]) {
                throwVerificationException(16, j, 0);
            }

            if (m_valIntArr[3][j] != VAL_INT_ARR[j]) {
                throwVerificationException(17, j, 0);
            }

            if (m_valLongArr[3][j] != VAL_LONG_ARR[j]) {
                throwVerificationException(18, j, 0);
            }
        }
    }

    private void throwVerificationException(final int p_pos, final int p_iteration, final int p_iteration2) {
        throw new IllegalStateException(
                "Verifying NetworkDebugMessage readPayload failed, pos " + p_pos + ", iteration " + p_iteration + ", iteration2 " + p_iteration2);
    }
}
