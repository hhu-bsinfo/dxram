package de.hhu.bsinfo.dxram.loader;

import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

public class ExternalDepsApp extends Application {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "ExternalDepsApp";
    }

    @Override
    public void main(String[] p_args) {
        System.out.println("Hello, my name is ExternalDepsApp!");
        try {
            ClassLoader.getSystemClassLoader().loadClass("de.hhu.bsinfo.dxapp.rest.cmd.requests.AppRunRequest");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void signalShutdown() {

    }
}
