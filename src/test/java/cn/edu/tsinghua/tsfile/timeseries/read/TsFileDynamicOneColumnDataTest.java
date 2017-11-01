package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.TsFileDynamicOneColumnData;
import org.junit.Assert;
import org.junit.Test;

/**
 * test the usage of TsFileDynamicOneColumnData
 */
public class TsFileDynamicOneColumnDataTest {

    private static final int MAXN = 100005;
    private static final float delta = 0.000001f;

    @Test
    public void testPutGetMethod() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.INT32, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        // Assert.assertEquals(data.timeArrayIdx, 100);

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
    }

    @Test
    public void testFloatMethod() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.FLOAT, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        for (int i = 0; i < MAXN; i++) {
            data.putFloat(i * 3.0f);
        }

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i * 3.0f, data.getFloat(i), delta);
        }
    }

    @Test
    public void testDoubleMethod() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.DOUBLE, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        for (int i = 0; i < MAXN; i++) {
            data.putDouble(i * 3.0);
        }

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i * 3.0, data.getDouble(i), delta);
        }
    }

    @Test
    public void testINT64Method() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.INT64, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        for (int i = 0; i < MAXN; i++) {
            data.putLong(i * 10L);
        }

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i * 10L, data.getLong(i));
        }
    }

    @Test
    public void testBooleanMethod() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.BOOLEAN, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        for (int i = 0; i < MAXN; i++) {
            data.putBoolean(i % 2 == 0);
        }

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i % 2 == 0, data.getBoolean(i));
        }
    }

    @Test
    public void testTextMethod() {
        TsFileDynamicOneColumnData data = new TsFileDynamicOneColumnData(TSDataType.TEXT, true);
        for (int i = 0; i < MAXN; i++) {
            data.putTime(i + 10);
        }
        for (int i = 0; i < MAXN; i++) {
            data.putBinary(Binary.valueOf(String.valueOf(i)));
        }

        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(i + 10, data.getTime(i));
        }
        for (int i = 0; i < MAXN; i++) {
            Assert.assertEquals(Binary.valueOf(String.valueOf(i)), data.getBinary(i));
        }
    }
}
