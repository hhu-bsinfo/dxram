package de.hhu.bsinfo.dxram.lib;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMServiceConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the LibraryService
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de, 17.07.2018
 */
public class LibraryServiceConfig extends AbstractDXRAMServiceConfig {
    /**
     * Constructor
     */
    public LibraryServiceConfig() {
        super(LibraryService.class, false, true);
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
