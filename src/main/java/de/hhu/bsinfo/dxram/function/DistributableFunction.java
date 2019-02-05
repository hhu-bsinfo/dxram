package de.hhu.bsinfo.dxram.function;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxutils.serialization.Importable;

@FunctionalInterface
public interface DistributableFunction<T> extends Serializable {
    void execute(final DXRAMServiceAccessor p_serviceAccessor, final T p_input);
}
