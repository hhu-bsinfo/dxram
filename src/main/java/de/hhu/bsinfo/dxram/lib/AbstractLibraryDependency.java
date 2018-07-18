package de.hhu.bsinfo.dxram.lib;

/**
 * Interface for dependencies (jar files) of a library
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de, 17.07.18
 */
public abstract class AbstractLibraryDependency {
    /**
     * Get a list of dependencies
     *
     * @return List of dependencies as string array
     */
    public abstract String[] getDependency();
}
