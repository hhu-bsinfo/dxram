package de.hhu.bsinfo.dxram;


import picocli.CommandLine;

import java.net.InetSocketAddress;

import de.hhu.bsinfo.dxram.commands.Root;
import de.hhu.bsinfo.dxram.commands.converter.InetSocketAddressConverter;

public final class Runner {

    private Runner() {}

    public static void main(final String[] p_args) {
        CommandLine cli = new CommandLine(new Root(p_args));
        cli.registerConverter(InetSocketAddress.class, new InetSocketAddressConverter(22222));
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        cli.parseWithHandlers(
                new CommandLine.RunLast().useOut(System.out),
                CommandLine.defaultExceptionHandler().useErr(System.err),
                p_args);
    }
}
