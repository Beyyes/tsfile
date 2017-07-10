package cn.edu.thu.tsfile.file.metadata.statistics;


import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class StringStatisticsTest {
    @Test
    public void testUpdate() {
        Statistics<String> stringStats = new StringStatistics();
        stringStats.updateStats("aaa");
        assertEquals(false, stringStats.isEmpty());
        stringStats.updateStats("bbb");
        assertEquals(false, stringStats.isEmpty());
        assertEquals("bbb", (String) stringStats.getMax());
        assertEquals("aaa", (String) stringStats.getMin());
    }

    @Test
    public void testMerge() {
        Statistics<String> stringStats1 = new StringStatistics();
        Statistics<String> stringStats2 = new StringStatistics();

        stringStats1.updateStats("aaa");
        stringStats1.updateStats("ccc");

        stringStats2.updateStats("ddd");

        Statistics<String> stringStats3 = new StringStatistics();
        stringStats3.mergeStatistics(stringStats1);
        assertEquals(false, stringStats3.isEmpty());
        assertEquals("ccc", (String) stringStats3.getMax());
        assertEquals("aaa", (String) stringStats3.getMin());

        stringStats3.mergeStatistics(stringStats2);
        assertEquals("ddd", (String) stringStats3.getMax());
        assertEquals("aaa", (String) stringStats3.getMin());
    }
}
