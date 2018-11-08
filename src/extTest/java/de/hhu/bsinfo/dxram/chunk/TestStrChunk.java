package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class TestStrChunk extends AbstractChunk {
    private String m_abc = "testtesttesttesttesttesttesttesttesttest";

    public String getAbc() {
        return m_abc;
    }

    public void setAbc(final String p_str) {
        m_abc = p_str;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_abc);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_abc = p_importer.readString(m_abc);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_abc);
    }
}
