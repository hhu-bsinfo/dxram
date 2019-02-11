package de.hhu.bsinfo.dxram.function;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.function.util.ParameterList;

public class IntAdderFunction implements DistributableFunction<ParameterList, ParameterList> {

    public static final String NAME = "de.hhu.bsinfo.dxram.intadder";

    @Override
    public ParameterList execute(DXRAMServiceAccessor p_serviceAccessor, ParameterList p_input) {
        int sum = p_input.stream()
                .mapToInt(Integer::valueOf)
                .sum();

        return new ParameterList(new String[]{String.valueOf(sum)});
    }
}
