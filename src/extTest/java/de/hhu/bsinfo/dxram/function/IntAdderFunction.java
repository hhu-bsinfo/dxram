package de.hhu.bsinfo.dxram.function;

import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.function.util.ParameterList;

public class IntAdderFunction implements DistributableFunction<ParameterList> {

    public static final String NAME = "de.hhu.bsinfo.dxram.intadder";

    @Override
    public void execute(DXRAMServiceAccessor p_serviceAccessor, ParameterList p_input) {
        int a = Integer.valueOf(p_input.get(0));
        int b = Integer.valueOf(p_input.get(1));

        BootService boot = p_serviceAccessor.getService(BootService.class);
        System.out.printf("[%04X] %d + %d = %d\n", boot.getNodeID(), a, b, a + b);
    }
}
