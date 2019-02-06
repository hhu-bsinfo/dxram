package de.hhu.bsinfo.dxram.function;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Importable;

@FunctionalInterface
public interface DistributableFunction<IN extends Distributable, OUT extends Distributable> extends Serializable {
    OUT execute(final DXRAMServiceAccessor p_serviceAccessor, final IN p_input);
}
