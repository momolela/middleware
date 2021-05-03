package com.momolela.cachecentre.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.Closeable;

public class CloseUtil {

    private static final Log LOG = LogFactory.getLog(CloseUtil.class);

    public static void close(Closeable closeable) {
        if (closeable != null)
            try {
                closeable.close();
            } catch (Exception e) {
                LOG.error("Unable to close " + closeable, e);
            }
    }

}
