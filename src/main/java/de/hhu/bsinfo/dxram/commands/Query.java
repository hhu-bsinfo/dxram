package de.hhu.bsinfo.dxram.commands;

import picocli.CommandLine;

@CommandLine.Command(
        name = "query",
        description = "Grants access to system resources.%n",
        subcommands = {Members.class})
public class Query implements Runnable {
    @Override
    public void run() {
        CommandLine.usage(this, System.out);
    }
}
