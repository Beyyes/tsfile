package cn.edu.tsinghua.tsfile.timeseries.read.support;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.util.ArrayList;

/**
 * TsFileDynamicOneColumnData is a self-defined data structure which is optimized for different type
 * of values. This class can be viewed as a collection which is more efficient than ArrayList.
 *
 * @author Jinrui Zhang
 */
public class TsFileDynamicOneColumnData {

    private static int CAPACITY = 1000;

    public int rowGroupIndex = 0;
    public long pageOffset = -1;
    public long leftSize = -1;
    public TSDataType dataType;
    public int curIdx;

    public int timeArrayIdx;  // the number of ArrayList in timeRet
    private int curTimeIdx;      // the index of current ArrayList in timeRet
    public int timeLength;   // the insert timestamp number of timeRet
    private int valueArrayIdx;// the number of ArrayList in valueRet
    private int curValueIdx;     // the index of current ArrayList in valueRet
    public int valueLength;  // the insert value number of valueRet

    public ArrayList<long[]> timeRet;
    public ArrayList<boolean[]> booleanRet;
    public ArrayList<int[]> intRet;
    public ArrayList<long[]> longRet;
    public ArrayList<float[]> floatRet;
    public ArrayList<double[]> doubleRet;
    public ArrayList<Binary[]> binaryRet;

    /**
     * Construction method.
     *
     * @param type       data type to record, one of <code>TSDataType</code>
     * @param recordTime whether to record timestamps for this TsFileDynamicOneColumnData
     */
    public TsFileDynamicOneColumnData(TSDataType type, boolean recordTime) {
        init(type, recordTime);
    }

    public void init(TSDataType type, boolean recordTime) {
        this.dataType = type;
        this.valueArrayIdx = 0;
        this.curValueIdx = 0;
        this.valueLength = 0;
        this.curIdx = 0;
        CAPACITY = TSFileConfig.dynamicDataSize;

        if (recordTime) {
            timeRet = new ArrayList<>();
            timeRet.add(new long[CAPACITY]);
            timeArrayIdx = 0;
            curTimeIdx = 0;
            timeLength = 0;
        }

        switch (dataType) {
            case BOOLEAN:
                booleanRet = new ArrayList<>();
                booleanRet.add(new boolean[CAPACITY]);
                break;
            case INT32:
                intRet = new ArrayList<>();
                intRet.add(new int[CAPACITY]);
                break;
            case INT64:
                longRet = new ArrayList<>();
                longRet.add(new long[CAPACITY]);
                break;
            case FLOAT:
                floatRet = new ArrayList<>();
                floatRet.add(new float[CAPACITY]);
                break;
            case DOUBLE:
                doubleRet = new ArrayList<>();
                doubleRet.add(new double[CAPACITY]);
                break;
            case TEXT:
                binaryRet = new ArrayList<>();
                binaryRet.add(new Binary[CAPACITY]);
                break;
            case ENUMS:
                intRet = new ArrayList<>();
                intRet.add(new int[CAPACITY]);
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public void putTime(long v) {
        if (curTimeIdx == CAPACITY) {
            this.timeRet.add(new long[CAPACITY]);
            timeArrayIdx++;
            curTimeIdx = 0;
        }
        (timeRet.get(timeArrayIdx))[curTimeIdx++] = v;
        timeLength++;
    }

    public void putBoolean(boolean v) {
        if (curValueIdx == CAPACITY) {
            if (this.booleanRet.size() <= valueArrayIdx + 1) {
                this.booleanRet.add(new boolean[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.booleanRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putInt(int v) {
        if (curValueIdx == CAPACITY) {
            if (this.intRet.size() <= valueArrayIdx + 1) {
                this.intRet.add(new int[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.intRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putLong(long v) {
        if (curValueIdx == CAPACITY) {
            if (this.longRet.size() <= valueArrayIdx + 1) {
                this.longRet.add(new long[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.longRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putFloat(float v) {
        if (curValueIdx == CAPACITY) {
            if (this.floatRet.size() <= valueArrayIdx + 1) {
                this.floatRet.add(new float[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.floatRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putDouble(double v) {
        if (curValueIdx == CAPACITY) {
            if (this.doubleRet.size() <= valueArrayIdx + 1) {
                this.doubleRet.add(new double[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.doubleRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public void putBinary(Binary v) {
        if (curValueIdx == CAPACITY) {
            if (this.binaryRet.size() <= valueArrayIdx + 1) {
                this.binaryRet.add(new Binary[CAPACITY]);
            }
            valueArrayIdx++;
            curValueIdx = 0;
        }
        (this.binaryRet.get(valueArrayIdx))[curValueIdx++] = v;
        valueLength++;
    }

    public boolean getBoolean(int idx) {
        rangeCheck(idx);
        return this.booleanRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setBoolean(int idx, boolean v) {
        rangeCheck(idx);
        this.booleanRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public int getInt(int idx) {
        rangeCheck(idx);
        return this.intRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setInt(int idx, int v) {
        rangeCheck(idx);
        this.intRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public long getLong(int idx) {
        rangeCheck(idx);
        return this.longRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setLong(int idx, long v) {
        rangeCheck(idx);
        this.longRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public float getFloat(int idx) {
        rangeCheck(idx);
        return this.floatRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setFloat(int idx, float v) {
        rangeCheck(idx);
        this.floatRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public double getDouble(int idx) {
        rangeCheck(idx);
        return this.doubleRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setDouble(int idx, double v) {
        rangeCheck(idx);
        this.doubleRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public Binary getBinary(int idx) {
        rangeCheck(idx);
        return this.binaryRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setBinary(int idx, Binary v) {
        this.binaryRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public long getTime(int idx) {
        rangeCheckForTime(idx);
        return this.timeRet.get(idx / CAPACITY)[idx % CAPACITY];
    }

    public void setTime(int idx, long v) {
        rangeCheckForTime(idx);
        this.timeRet.get(idx / CAPACITY)[idx % CAPACITY] = v;
    }

    public String getStringValue(int idx) {
        switch (dataType) {
            case BOOLEAN:
                return String.valueOf(getBoolean(idx));
            case INT32:
                return String.valueOf(getInt(idx));
            case INT64:
                return String.valueOf(getLong(idx));
            case FLOAT:
                return String.valueOf(getFloat(idx));
            case DOUBLE:
                return String.valueOf(getDouble(idx));
            case TEXT:
                return String.valueOf(getBinary(idx));
            case ENUMS:
                return String.valueOf(getBinary(idx));
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    public String getStringTimeValuePair(int idx) {
        String v;
        switch (dataType) {
            case BOOLEAN:
                v = String.valueOf(getBoolean(idx));
                break;
            case INT32:
                v = String.valueOf(getInt(idx));
                break;
            case INT64:
                v = String.valueOf(getLong(idx));
                break;
            case FLOAT:
                v = String.valueOf(getFloat(idx));
                break;
            case DOUBLE:
                v = String.valueOf(getDouble(idx));
                break;
            case TEXT:
                v = String.valueOf(getBinary(idx));
                break;
            case ENUMS:
                v = String.valueOf(getBinary(idx));
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
        String t = String.valueOf(getTime(idx));
        StringBuffer sb = new StringBuffer();
        sb.append(t);
        sb.append("\t");
        sb.append(v);
        return sb.toString();
    }

    /**
     * Add all time and value from another TsFileDynamicOneColumnData to itself.
     *
     * @param col dynamicOneColumnData to be merged
     */
    public void mergeRecord(TsFileDynamicOneColumnData col) {
        for (int i = 0; i < col.timeLength; i++) {
            putTime(col.getTime(i));
        }
        switch (dataType) {
            case BOOLEAN:
                for (int i = 0; i < col.valueLength; i++) {
                    putBoolean(col.getBoolean(i));
                }
                break;
            case INT32:
                for (int i = 0; i < col.valueLength; i++) {
                    putInt(col.getInt(i));
                }
                break;
            case INT64:
                for (int i = 0; i < col.valueLength; i++) {
                    putLong(col.getLong(i));
                }
                break;
            case FLOAT:
                for (int i = 0; i < col.valueLength; i++) {
                    putFloat(col.getFloat(i));
                }
                break;
            case DOUBLE:
                for (int i = 0; i < col.valueLength; i++) {
                    putDouble(col.getDouble(i));
                }
                break;
            case TEXT:
                for (int i = 0; i < col.valueLength; i++) {
                    putBinary(col.getBinary(i));
                }
                break;
            case ENUMS:
                for (int i = 0; i < col.valueLength; i++) {
                    putBinary(col.getBinary(i));
                }
                break;
            default:
                throw new UnSupportedDataTypeException(String.valueOf(dataType));
        }
    }

    /**
     * Checks whether the given index is valid.
     * If not, throws an appropriate runtime exception.
     */
    private void rangeCheck(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("Index is negative: " + idx);
        }
        if (idx >= valueLength) {
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + valueLength);
        }
    }

    /**
     * Checks whether the given index is valid.
     * If not, throws an appropriate runtime exception.
     */
    private void rangeCheckForTime(int idx) {
        if (idx < 0) {
            throw new IndexOutOfBoundsException("Index is negative: " + idx);
        }
        if (idx >= timeLength) {
            throw new IndexOutOfBoundsException("Index : " + idx + ". Length : " + valueLength);
        }
    }

    public void clearData() {
        this.init(dataType, true);
    }

    public TsFileDynamicOneColumnData sub(int startPos) {
        return sub(startPos, this.valueLength - 1);
    }

    /**
     * Extract the needed data between start position and end position.
     *
     * @param startPos start position of index
     * @param endPos end position of index
     * @return the new TsFileDynamicOneColumnData whose data is equals to position startPos and position endPos
     */
    public TsFileDynamicOneColumnData sub(int startPos, int endPos) {
        TsFileDynamicOneColumnData subRes = new TsFileDynamicOneColumnData(dataType, true);
        for (int i = startPos; i <= endPos; i++) {
            subRes.putTime(getTime(i));
            switch (dataType) {
                case BOOLEAN:
                    putBoolean(this.getBoolean(i));
                    break;
                case INT32:
                    putInt(this.getInt(i));
                    break;
                case INT64:
                    putLong(this.getLong(i));
                    break;
                case FLOAT:
                    putFloat(this.getFloat(i));
                    break;
                case DOUBLE:
                    putDouble(this.getDouble(i));
                    break;
                case TEXT:
                    putBinary(this.getBinary(i));
                    break;
                case ENUMS:
                    putBinary(this.getBinary(i));
                    break;
                default:
                    throw new UnSupportedDataTypeException(String.valueOf(dataType));
            }
        }
        return subRes;
    }

    public void plusRowGroupIndexAndInitPageOffset() {
        this.rowGroupIndex++;
        //RowGroupIndex's change means that The pageOffset should be updateTo the value in next RowGroup.
        //But we don't know the value, so set the pageOffset to -1. And we calculate the accuracy value
        //in the reading procedure.
        this.pageOffset = -1;
    }

    public int getRowGroupIndex() {
        return this.rowGroupIndex;
    }

}
