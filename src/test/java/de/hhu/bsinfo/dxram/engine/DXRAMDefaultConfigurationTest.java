package de.hhu.bsinfo.dxram.engine;

import java.io.File;

import org.junit.Assert;
import org.junit.Test;

import de.hhu.bsinfo.dxram.DXRAM;

public class DXRAMDefaultConfigurationTest {
    private static final String CONFIG_PATH = "/tmp/dxram_test.config";

    @Test
    public void test() throws Exception {
        File file = new File(CONFIG_PATH);

        if (file.exists()) {
            if (!file.delete()) {
                throw new RuntimeException("Deleting file " + CONFIG_PATH + " failed.");
            }
        }

        DXRAM dxram = new DXRAM();

        DXRAMConfig config1 = dxram.createDefaultConfigInstance();
        DXRAMConfig config2 = dxram.createDefaultConfigInstance();

        DXRAMConfigBuilderJsonFile builder = new DXRAMConfigBuilderJsonFile(CONFIG_PATH, true);
        DXRAMConfigBuilderJsonFile builder2 = new DXRAMConfigBuilderJsonFile(CONFIG_PATH, false);

        config1 = builder.build(config1);
        config1 = builder2.build(config1);

        Assert.assertEquals(config2.toString(), config1.toString());
    }
}
