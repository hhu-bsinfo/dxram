package de.hhu.bsinfo.dxram.ms;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class TestIncChunk extends AbstractChunk {
    private int m_counter;

    public TestIncChunk() {

    }

    public void incCounter() {
        m_counter++;
    }

    public int getCounter() {
        return m_counter;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeInt(m_counter);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_counter = p_importer.readInt(m_counter);
    }

    @Override
    public int sizeofObject() {
        return Integer.BYTES;
    }
}
