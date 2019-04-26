package de.hhu.bsinfo.dxram.util;

import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class DistributableInteger extends DistributableValue<Integer> {

    public DistributableInteger() {}

    public DistributableInteger(Integer p_value) {
        m_value = p_value;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(m_value);
    }

    @Override
    public void importObject(Importer p_importer) {
        if (m_value == null) {
            m_value = 0;
        }
        m_value = p_importer.readInt(m_value);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}
