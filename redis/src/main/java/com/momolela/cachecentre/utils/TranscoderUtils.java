package com.momolela.cachecentre.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class TranscoderUtils {

    private static final Log LOG = LogFactory.getLog(TranscoderUtils.class);


    /**
     * @param o
     * @return
     * @描述 : 将对象转码为byte
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午8:53:21
     */
    public static byte[] encodeObject(Object o) {
        return compress(serialize(o));
    }

    /**
     * @param b
     * @return
     * @描述 : 将byte解码转换为对象
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午9:03:18
     */
    public static Object decodeObject(byte[] b) {
        return deserialize(decompress(b));
    }

    /**
     * @param in
     * @return
     * @throws RuntimeException
     * @描述 : 转码
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午9:03:41
     */
    private static byte[] compress(byte[] in) throws RuntimeException {
        if (in == null) {
            throw new NullPointerException("Can't compress null");
        }
        ByteArrayOutputStream bos = null;
        GZIPOutputStream gz = null;
        try {
            bos = new ByteArrayOutputStream();
            gz = new GZIPOutputStream(bos);
            gz.write(in);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("IO exception compressing data", e);
        } finally {
            CloseUtil.close(gz);
            CloseUtil.close(bos);
        }
        return bos.toByteArray();
    }

    /**
     * @param in
     * @return
     * @throws RuntimeException
     * @描述 : 解码
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午9:03:51
     */
    private static byte[] decompress(byte[] in) throws RuntimeException {
        if (in != null) {
            ByteArrayOutputStream byteArrayOutputStream = null;
            ByteArrayInputStream byteArrayInputStream = null;
            GZIPInputStream gzipInputStream = null;
            try {
                byteArrayInputStream = new ByteArrayInputStream(in);
                byteArrayOutputStream = new ByteArrayOutputStream();
                gzipInputStream = new GZIPInputStream(byteArrayInputStream);
                byte[] buf = new byte[8192];
                int r = -1;
                while ((r = gzipInputStream.read(buf)) > 0) {
                    byteArrayOutputStream.write(buf, 0, r);
                }
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException("IO exception decompress data", e);
            } finally {
                CloseUtil.close(gzipInputStream);
                CloseUtil.close(byteArrayInputStream);
                CloseUtil.close(byteArrayOutputStream);
            }
            return byteArrayOutputStream.toByteArray();
        } else {
            return null;
        }
    }

    /**
     * @param o
     * @return
     * @描述 : 序列化
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午9:04:01
     */
    private static byte[] serialize(Object o) {
        if (o == null) {
            throw new NullPointerException("Can't serialize null");
        }
        ByteArrayOutputStream byteArrayOutputStream = null;
        ObjectOutputStream objectOutputStream = null;
        try {
            byteArrayOutputStream = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(o);
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            throw new RuntimeException("Non-serializable object", e);
        } finally {
            CloseUtil.close(objectOutputStream);
            CloseUtil.close(byteArrayOutputStream);
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * @param in
     * @return
     * @描述 : 反序列化
     * @创建者：liushengsong
     * @创建时间： 2014-6-16上午9:04:25
     */
    private static Object deserialize(byte[] in) {
        if (in != null) {
            ByteArrayInputStream byteArrayInputStream = null;
            ObjectInputStream objectInputStream = null;
            Object obj = null;
            try {
                byteArrayInputStream = new ByteArrayInputStream(in);
                objectInputStream = new ObjectInputStream(
                        byteArrayInputStream);
                obj = objectInputStream.readObject();
            } catch (IOException | ClassNotFoundException e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e);
            } finally {
                CloseUtil.close(objectInputStream);
                CloseUtil.close(byteArrayInputStream);
            }
            return obj;
        } else {
            return null;
        }
    }

}
