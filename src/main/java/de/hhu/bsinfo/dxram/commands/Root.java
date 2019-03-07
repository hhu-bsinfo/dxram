package de.hhu.bsinfo.dxram.commands;

import picocli.CommandLine;

import de.hhu.bsinfo.dxram.DXRAMMain;

@CommandLine.Command(
        name = "dxram",
        description = "A distributed key-value store",
        subcommands = { Start.class , Query.class , Submit.class}
)
public class Root implements Runnable{

    private final String[] m_args;

    public Root(final String[] p_args) {
        m_args = p_args;
    }

    @Override
    public void run() {
        // Start DXRAM the old way if no subcommand was specified
        DXRAMMain.main(m_args);
    }
}
