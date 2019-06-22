package de.hhu.bsinfo.dxram.app;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import de.hhu.bsinfo.dxram.engine.Component;
import de.hhu.bsinfo.dxram.engine.Module;
import de.hhu.bsinfo.dxram.engine.DXRAMConfig;
import de.hhu.bsinfo.dxram.engine.DXRAMJNIManager;
import de.hhu.bsinfo.dxram.engine.ModuleConfig;
import de.hhu.bsinfo.dxram.loader.LoaderComponent;
import de.hhu.bsinfo.dxram.plugin.PluginComponent;
import de.hhu.bsinfo.dxutils.dependency.Dependency;;

/**
 * Component to run applications locally on the DXRAM instance with access to all exposed services
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
@Module.Attributes(supportsSuperpeer = false, supportsPeer = true)
public class ApplicationComponent extends Component<ModuleConfig> {

    @Dependency
    private PluginComponent m_plugin;
    @Dependency
    private LoaderComponent m_loader;

    private final HashMap<String, Class<? extends Application>> m_applicationClasses = new HashMap<>();
    private ApplicationRunner m_runner;

    private static final String NO_APP = "";

    /**
     * Start an application
     *
     * @param p_class
     *         Fully qualified class name of application to start
     * @param p_args
     *         Arguments for application
     * @return True if starting application was successful, false on error
     */
    public boolean startApplication(final String p_class, final String[] p_args) {
        if (!m_applicationClasses.containsKey(p_class)) {
            try {
                m_loader.getJar(p_class);
                Class<?> clazz = m_loader.getM_loader().loadClass(p_class);

                if (Application.class.isAssignableFrom(clazz)) {
                    registerApplicationClass((Class<? extends Application>) clazz);
                }else {
                    return false;
                }
            } catch(ClassNotFoundException e){
                LOGGER.error(String.format("Application class %s was not found", p_class));
                return false;
            }
        }

        return m_runner.startApplication(p_class, p_args);
    }

    /**
     * Shutdown a running application. This triggers the shutdown signal to allow the application
     * to initiate a soft shutdown
     *
     * @param p_class
     *         Fully qualified class name of application to shut down
     */
    public void shutdownApplication(final String p_class) {
        m_runner.shutdownApplication(p_class);
    }

    /**
     * Get a list of loaded application classes (available for starting)
     *
     * @return List of application classes loaded
     */
    public List<String> getLoadedApplicationClasses() {
        return new ArrayList<>(m_applicationClasses.keySet());
    }

    /**
     * Get a list of currently running applications
     *
     * @return List of currently running applications
     */
    public List<String> getApplicationsRunning() {
        return m_runner.getApplicationsRunning();
    }

    public Collection<ApplicationProcess> getRunningProcesses() {
        return m_runner.getRunningProcesses();
    }

    /**
     * Register an application class at the loader (external). Allows registering in-code
     * application classes without having to load jars (e.g. for testing).
     *
     * @param p_class
     *         Application class to register
     */
    public void registerApplicationClass(final Class<? extends Application> p_class) {
        m_applicationClasses.put(p_class.getName(), p_class);
    }

    public String addApplication(final String p_archiveName) {
        String pluginDir = m_plugin.getConfig().getPluginsPath();
        Path pluginPath = Paths.get(pluginDir, p_archiveName);

        LOGGER.debug("Adding new plugin %s", pluginPath.toString());

        m_plugin.add(pluginPath);

        List<Class<? extends Application>> applicationClasses = m_plugin.getAllSubClasses(
                Application.class, p_archiveName);

        if (applicationClasses.isEmpty()) {
            return NO_APP;
        }

        Class<? extends Application> applicationClass = applicationClasses.get(0);

        registerApplicationClass(applicationClass);

        return applicationClass.getName();
    }

    @Override
    protected boolean initComponent(final DXRAMConfig p_config, final DXRAMJNIManager p_jniManager) {
        List<Class<? extends Application>> applicationClasses = m_plugin.getAllSubClasses(
                Application.class);

        for (Class<? extends Application> clazz : applicationClasses) {
            m_applicationClasses.put(clazz.getName(), clazz);
        }

        m_runner = new ApplicationRunner(m_applicationClasses, getParentEngine().getVersion(), getParentEngine());

        return true;
    }

    @Override
    protected boolean shutdownComponent() {
        m_runner.shutdown();
        m_runner = null;

        m_applicationClasses.clear();

        return true;
    }
}
