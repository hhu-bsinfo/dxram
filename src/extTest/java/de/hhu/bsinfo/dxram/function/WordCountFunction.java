package de.hhu.bsinfo.dxram.function;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;

import de.hhu.bsinfo.dxram.engine.ServiceProvider;
import de.hhu.bsinfo.dxutils.data.DistributableHashMap;
import de.hhu.bsinfo.dxutils.data.holder.DistributableInteger;
import de.hhu.bsinfo.dxutils.data.holder.DistributableLong;
import de.hhu.bsinfo.dxutils.data.holder.DistributableString;

public class WordCountFunction implements DistributableFunction<DistributableString, DistributableHashMap<String, Long>> {

    public static final String NAME = "de.hhu.bsinfo.dxram.wordcount";

    @Override
    public DistributableHashMap<String, Long> execute(ServiceProvider p_serviceAccessor, DistributableString p_input) {
        return Arrays.stream(p_input.getValue().split("\\W+"))
                .collect(Collectors.groupingBy(
                       Function.identity(),
                       () -> new DistributableHashMap<>(DistributableString::new, DistributableLong::new),
                       Collectors.counting()));
    }
}
