package de.hhu.bsinfo.dxram;


import picocli.CommandLine;

import de.hhu.bsinfo.dxram.commands.Root;

public final class Runner {

    private Runner() {}

    public static void main(final String[] p_args) {
        CommandLine cli = new CommandLine(new Root(p_args));
        cli.setCaseInsensitiveEnumValuesAllowed(true);
        cli.parseWithHandlers(
                new CommandLine.RunLast().useOut(System.out),
                CommandLine.defaultExceptionHandler().useErr(System.err),
                p_args);
    }
}
