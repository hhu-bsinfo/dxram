package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;

public class TableCountResponse extends Response {
    @Getter
    private int m_tableCount;

    public TableCountResponse() {
        super();
    }

    public TableCountResponse(final TableCountRequest p_request, final int p_tableCount) {
        super(p_request, LoaderMessages.SUBTYPE_TABLE_COUNT_RESPONSE);
        m_tableCount = p_tableCount;
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_tableCount);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_tableCount = p_importer.readInt(m_tableCount);
    }
}
