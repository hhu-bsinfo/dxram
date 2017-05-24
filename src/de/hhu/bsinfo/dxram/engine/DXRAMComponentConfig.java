package de.hhu.bsinfo.dxram.engine;

import com.google.gson.annotations.Expose;

/**
 * Provides configuration values for a component. Use this as a base class for all components to add further configuration values
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.05.2017
 */
public class DXRAMComponentConfig {
    @Expose
    private String m_class;

    @Expose
    private String m_componentClass;

    @Expose
    private boolean m_enabledForSuperpeer;

    @Expose
    private boolean m_enabledForPeer;

    /**
     * Constructor
     *
     * @param p_class
     *         Class extending the abstract component class of this configuration
     * @param p_enabledForSuperpeer
     *         True to enable the component if the node is a superpeer, false to disable
     * @param p_enabledForPeer
     *         True to enable the component if the node is a peer, false to disable
     */
    protected DXRAMComponentConfig(final Class<? extends AbstractDXRAMComponent> p_class, final boolean p_enabledForSuperpeer, final boolean p_enabledForPeer) {
        m_class = getClass().getName();
        m_componentClass = p_class.getSimpleName();
        m_enabledForSuperpeer = p_enabledForSuperpeer;
        m_enabledForPeer = p_enabledForPeer;
    }

    /**
     * Get the fully qualified class name of the config class
     */
    public String getClassName() {
        return m_class;
    }

    /**
     * Get the fully qualified class name of the component of this configuration
     */
    public String getComponentClass() {
        return m_componentClass;
    }

    /**
     * True to enable the component if the node is a superpeer, false to disable
     */
    public boolean isEnabledForSuperpeer() {
        return m_enabledForSuperpeer;
    }

    /**
     * True to enable the component if the node is a peer, false to disable
     */
    public boolean isEnabledForPeer() {
        return m_enabledForPeer;
    }
}
