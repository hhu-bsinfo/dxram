package de.hhu.bsinfo.dxram.mem;

import de.hhu.bsinfo.dxram.engine.DXRAMRuntimeException;

public class OutOfKeyValueStoreMemoryException extends DXRAMRuntimeException {
    OutOfKeyValueStoreMemoryException(final MemoryManagerComponent.Status p_status) {
        super("Out of key value store memory, memory status:\n" + p_status);
    }
}
