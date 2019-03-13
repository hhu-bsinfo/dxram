package de.hhu.bsinfo.dxram.job;

import org.junit.Assert;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Minimal job to test serialization of jobs for remote submission.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 05.02.2019
 */
public class JobRemoteSerializeTest extends Job {
    private static final int VAL1 = 4321;
    private static long VAL2 = 88282828282L;
    private static final byte VAL3 = 123;
    private static final String VAL5 = "asdfghjkqwert1235";

    private int m_value;
    private long m_value2;
    private byte m_value3;
    private short[] m_value4;
    private String m_value5;

    public JobRemoteSerializeTest() {
        m_value = VAL1;
        m_value2 = VAL2;
        m_value3 = VAL3;
        m_value4 = new short[15];
        m_value5 = VAL5;
    }

    @Override
    public void execute() {
        Assert.assertEquals(VAL1, m_value);
        Assert.assertEquals(VAL2, m_value2);
        Assert.assertEquals(VAL3, m_value3);
        Assert.assertEquals(VAL5, m_value5);
    }

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        m_value = p_importer.readInt(m_value);
        m_value2 = p_importer.readLong(m_value2);
        m_value3 = p_importer.readByte(m_value3);
        m_value4 = p_importer.readShortArray(m_value4);
        m_value5 = p_importer.readString(m_value5);
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);

        p_exporter.writeInt(m_value);
        p_exporter.writeLong(m_value2);
        p_exporter.writeByte(m_value3);
        p_exporter.writeShortArray(m_value4);
        p_exporter.writeString(m_value5);
    }

    @Override
    public int sizeofObject() {
        return super.sizeofObject() + Integer.BYTES + Long.BYTES + Byte.BYTES +
                ObjectSizeUtil.sizeofShortArray(m_value4) + ObjectSizeUtil.sizeofString(m_value5);
    }
}
