package de.hhu.bsinfo.dxram.app;

/**
 * Interface for dependencies (jar files) of an application
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public abstract class AbstractApplicationDependency {
    /**
     * Get a list of dependencies
     *
     * @return List of dependencies as string array
     */
    public abstract String[] getDependency();
}
