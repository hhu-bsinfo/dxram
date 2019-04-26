package de.hhu.bsinfo.dxram.loader;

public class DistributedSystemLoader extends ClassLoader {
    private DistributedLoader m_distributedLoader;

    public DistributedSystemLoader(ClassLoader p_parent) {
        super(p_parent);
    }

    @Override
    protected Class<?> findClass(String p_name) throws ClassNotFoundException {
        if (m_distributedLoader != null) {
            return m_distributedLoader.findClass(p_name);
        }else {
            return super.findClass(p_name);
        }
    }

    public void setDistributedLoader(DistributedLoader p_distributedLoader) {
        m_distributedLoader = p_distributedLoader;
    }
}
