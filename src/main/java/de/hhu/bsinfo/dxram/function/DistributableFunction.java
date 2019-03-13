package de.hhu.bsinfo.dxram.function;

import java.io.Serializable;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxutils.serialization.Distributable;

@FunctionalInterface
public interface DistributableFunction<IN extends Distributable, OUT extends Distributable> extends Serializable {
    OUT execute(final ServiceProvider p_serviceAccessor, final IN p_input);
}
