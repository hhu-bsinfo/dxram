package de.hhu.bsinfo.dxram;


import picocli.CommandLine;

import de.hhu.bsinfo.dxram.commands.Root;

public final class Runner {

    private Runner() {}

    public static void main(final String[] p_args) {
        CommandLine.run(new Root(p_args), p_args);
    }
}
