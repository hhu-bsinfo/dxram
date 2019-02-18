package de.hhu.bsinfo.dxram.ms.script;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class TaskScriptNodeData implements TaskScriptNode {
    private String m_name;
    private byte[] m_data;

    public TaskScriptNodeData() {

    }

    public String getName() {
        return m_name;
    }

    public byte[] getData() {
        return m_data;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        p_exporter.writeString(m_name);
        p_exporter.writeByteArray(m_data);
    }

    @Override
    public void importObject(final Importer p_importer) {
        m_name = p_importer.readString(m_name);
        m_data = p_importer.readByteArray(m_data);
    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofString(m_name) + ObjectSizeUtil.sizeofByteArray(m_data);
    }
}
