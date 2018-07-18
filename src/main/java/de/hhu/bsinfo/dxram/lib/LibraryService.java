package de.hhu.bsinfo.dxram.lib;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;

/**
 * Service to run libraries locally on the DXRAM instance with access to all exposed services
 *
 * @author Kai Neyenhuys, kai.neyenhuys@hhu.de, 17.07.18
 */
public class LibraryService extends AbstractDXRAMService<LibraryServiceConfig> {
    private LibraryComponent m_library;

    private List<AbstractLibrary> m_libraries;

    /**
     * Constructor
     */
    public LibraryService() {
        super("lib", LibraryServiceConfig.class);
    }

    @Override
    protected boolean supportsSuperpeer() {
        return false;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_library = p_componentAccessor.getComponent(LibraryComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        List<Class<? extends AbstractLibrary>> libraryClasses = m_library.getLibraryClasses();
        m_libraries = new ArrayList<>();
        LOGGER.debug("Loading %d libraries...", libraryClasses.size());
        DXRAMVersion curVersion = getParentEngine().getVersion();

        for (Class<? extends AbstractLibrary> libClass : libraryClasses) {
            // check if a configuration file for the library exists
            AbstractLibrary lib;
            File configFile = new File(m_library.getLibraryPath() + '/' + libClass.getSimpleName() + ".conf");

            if (configFile.exists()) {
                lib = loadFromConfiguration(libClass, configFile.getAbsolutePath());

                // loading failed
                if (lib == null) {
                    continue;
                }
            } else {
                try {
                    lib = libClass.newInstance();
                } catch (IllegalAccessException | InstantiationException e) {
                    LOGGER.error("Creating instance of library class %s failed: %s", libClass.getName(),
                            e.getMessage());
                    continue;
                }

                // generate configuration file
                if (lib.useConfigurationFile()) {
                    LOGGER.debug("Missing config for library '%s', creating...", lib.getLibraryName());

                    if (!createDefaultConfiguration(lib, configFile.getAbsolutePath())) {
                        continue;
                    }
                }
            }

            if (!lib.isEnabled()) {
                LOGGER.debug("Library '%s' disabled", lib.getLibraryName());
                continue;
            }

            // verify if built against current version
            DXRAMVersion version = lib.getBuiltAgainstVersion();

            if (version.getMajor() < curVersion.getMajor()) {
                LOGGER.error("Cannot load library '%s', major version (%s) not matching current DXRAM version " +
                                "(%s), update your library and ensure compatibility with the current DXRAM version",
                        lib.getLibraryName(), version, curVersion);
                continue;
            }

            if (version.getMinor() < curVersion.getMinor()) {
                LOGGER.warn(
                        "Library '%s' built against DXRAM version %s, current version %s. Your library " +
                                "might need minor updating to ensure full compatibility with the current version.",
                        lib.getLibraryName(), version, curVersion);
            }

            lib.setEngine(getParentEngine());
            m_libraries.add(lib);
        }

        return true;
    }

    @Override
    protected boolean shutdownService() {
        LOGGER.debug("Termiante all initiated libraries (%d)...", m_libraries.size());

        for (AbstractLibrary lib : m_libraries) {
            lib.terminate();
        }

        LOGGER.debug("All libraries terminated");
        m_libraries.clear();
        return true;
    }

    @Override
    protected void engineInitFinished() {
        // init all libraries
        for (AbstractLibrary lib : m_libraries) {
            lib.init();
        }
    }

    @Override
    protected boolean isEngineAccessor() {
        // access the engine to hook it to the libraries
        return true;
    }

    /**
     * Load an existing configuration of an library
     *
     * @param p_libClass
     *         Class of the library
     * @param p_configFilePath
     *         Path to existing configuration file
     * @return Library instance with configuration values loaded or null if loading failed
     */
    private AbstractLibrary loadFromConfiguration(final Class<? extends AbstractLibrary> p_libClass,
            final String p_configFilePath) {
        AbstractLibrary lib;
        LOGGER.info("Loading configuration '%s'...", p_configFilePath);
        Gson gson = LibraryGsonContext.createGsonInstance(p_libClass);
        JsonElement element;

        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            LOGGER.error("Could not load configuration '%s': %s", p_configFilePath, e.getMessage());
            return null;
        }

        try {
            lib = gson.fromJson(element, AbstractLibrary.class);
        } catch (final Exception e) {
            LOGGER.error("Loading configuration '%s' failed: %s", p_configFilePath, e.getMessage());
            return null;
        }

        return lib;
    }

    /**
     * Create a default configuration for a library
     *
     * @param p_lib
     *         Library to create a default configuration for
     * @param p_configFilePath
     *         Path for configuration file
     * @return True if successful and config file was created, false on error
     */
    private boolean createDefaultConfiguration(final AbstractLibrary p_lib, final String p_configFilePath) {
        File file = new File(p_configFilePath);

        if (file.exists()) {
            if (!file.delete()) {
                LOGGER.error("Deleting existing config file %s failed", file);

                return false;
            }
        }

        try {
            if (!file.createNewFile()) {
                LOGGER.error("Creating new config file %s failed", file);
                return false;
            }
        } catch (final IOException e) {
            LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());
            return false;
        }

        Gson gson = LibraryGsonContext.createGsonInstance(p_lib.getClass());
        String jsonString = gson.toJson(p_lib);

        try {
            PrintWriter writer = new PrintWriter(file);
            writer.print(jsonString);
            writer.close();
        } catch (final FileNotFoundException e) {
            // we can ignored this here, already checked
        }

        return true;
    }

    /**
     * Get started library
     *
     * @param p_class
     *         Needed library
     * @return Instance of started library
     */
    public <T extends AbstractLibrary> T getLibrary(final Class<T> p_class) {
        String simpleName = p_class.getSimpleName();

        for (AbstractLibrary lib : m_libraries) {
            if (lib.getClass().getSimpleName().equals(simpleName)) {
                return (T) lib;
            }
        }

        LOGGER.warn("Library not available %s", p_class);
        return null;
    }
}