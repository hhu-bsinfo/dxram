package de.hhu.bsinfo.dxram.lib;

import com.google.gson.annotations.Expose;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponentConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;

/**
 * Config for the Library Component
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de 17.07.2018
 */
public class LibraryComponentConfig extends AbstractDXRAMComponentConfig {
    @Expose
    private String m_libraryPath = "dxlib";

    /**
     * Constructor
     */
    public LibraryComponentConfig() {
        super(LibraryComponent.class, false, true);
    }

    /**
     * Path for library jar packages
     */
    public String getLibraryPath() {
        return m_libraryPath;
    }

    @Override
    protected boolean verify(final DXRAMContext.Config p_config) {
        return true;
    }
}
