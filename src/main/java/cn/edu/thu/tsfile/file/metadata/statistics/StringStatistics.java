package cn.edu.thu.tsfile.file.metadata.statistics;

import cn.edu.thu.tsfile.common.utils.BytesUtils;

/**
 * Statistics for string type
 * @author CGF
 */
public class StringStatistics extends Statistics<String>{
    private String max;
    private String min;

    @Override
    public void setMinMaxFromBytes(byte[] minBytes, byte[] maxBytes) {
        max = BytesUtils.bytesToString(maxBytes);
        min = BytesUtils.bytesToString(minBytes);
    }

    @Override
    public String getMin() {
        return min;
    }

    @Override
    public String getMax() {
        return max;
    }

    public void initializeStats(String min, String max) {
        this.min = min;
        this.max = max;
    }

    @Override
    protected void mergeStatisticsMinMax(Statistics<?> stats) {
        StringStatistics stringStats = (StringStatistics) stats;
        if (isEmpty) {
            initializeStats(stringStats.getMin(), stringStats.getMax());
            isEmpty = false;
        } else {
            updateStats(stringStats.getMin(), stringStats.getMax());
        }
    }

    @Override
    public void updateStats(String value) {
        if (isEmpty) {
            initializeStats(value, value);
            isEmpty = false;
        } else {
            updateStats(value, value);
            isEmpty = false;
        }
    }

    private void updateStats(String minValue, String maxValue) {
        if (minValue.compareTo(min) < 0) {
            min = minValue;
        }
        if (maxValue.compareTo(max) > 0) {
            max = maxValue;
        }
    }

    @Override
    public byte[] getMaxBytes() {
        return BytesUtils.StringToBytes(max);
    }

    @Override
    public byte[] getMinBytes() {
        return BytesUtils.StringToBytes(min);
    }
}
