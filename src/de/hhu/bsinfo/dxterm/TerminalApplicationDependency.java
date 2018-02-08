package de.hhu.bsinfo.dxterm;

import de.hhu.bsinfo.dxram.app.AbstractApplicationDependency;

public class TerminalApplicationDependency extends AbstractApplicationDependency {
    @Override
    public String[] getDependency() {
        return new String[] {"lib/jline-2.15.jar"};
    }
}
