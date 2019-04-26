package de.hhu.bsinfo.dxram.function;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxram.util.DistributableHashMap;
import de.hhu.bsinfo.dxram.util.DistributableInteger;
import de.hhu.bsinfo.dxram.util.DistributableString;

public class MapInverterFunction implements DistributableFunction<DistributableHashMap<String, Integer>, DistributableHashMap<Integer, String>> {

    public static final String NAME = "de.hhu.bsinfo.dxram.mapinverter";

    @Override
    public DistributableHashMap<Integer, String> execute(ServiceProvider p_serviceAccessor,
            DistributableHashMap<String, Integer> p_input) {

        DistributableHashMap<Integer, String> inverted =
                new DistributableHashMap<>(DistributableInteger::new, DistributableString::new);

        p_input.forEach((key, value) -> inverted.put(value, key));

        return inverted;
    }
}
