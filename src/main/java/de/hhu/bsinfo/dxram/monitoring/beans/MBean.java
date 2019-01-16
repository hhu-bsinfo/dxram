package de.hhu.bsinfo.dxram.monitoring.beans;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

abstract public class MBean {

    private final ObjectName m_objectName;

    MBean(String p_name) {
        try {
            m_objectName = new ObjectName("de.hhu.bsinfo.dxram:type=" + p_name);
        } catch (MalformedObjectNameException e) {
            throw new RuntimeException(e);
        }
    }

    public ObjectName getObjectName() {
        return m_objectName;
    }
}
