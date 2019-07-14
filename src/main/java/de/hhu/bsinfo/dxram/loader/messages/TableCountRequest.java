package de.hhu.bsinfo.dxram.loader.messages;

import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class TableCountRequest extends Request {
    public TableCountRequest() {
        super();
    }

    public TableCountRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_TABLE_COUNT_REQUEST);
    }
}
