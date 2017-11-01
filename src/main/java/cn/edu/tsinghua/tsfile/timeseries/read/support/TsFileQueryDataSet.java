package cn.edu.tsinghua.tsfile.timeseries.read.support;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.BatchReadRecordGenerator;
import cn.edu.tsinghua.tsfile.timeseries.read.query.CrossQueryTimeGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;


public class TsFileQueryDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(TsFileQueryDataSet.class);
    private static final char PATH_SPLITTER = '.';

    // Time Generator for Cross Query when using batching read
    public CrossQueryTimeGenerator timeQueryDataSet;
    public LinkedHashMap<String, TsFileDynamicOneColumnData> mapRet;
    protected BatchReadRecordGenerator batchReaderRetGenerator;

    protected PriorityQueue<Long> heap; // special for save time values when processing cross getIndex
    protected TsFileDynamicOneColumnData[] cols; // the content of cols equals to mapRet
    protected int[] idxs; // idxs[i] stores the curIdx of cols[i]
    protected String[] deltaObjectIds;
    protected String[] measurementIds;

    protected HashMap<Long, Integer> timeMap; // timestamp occurs time
    protected int size;
    protected boolean ifInit = false;
    protected RowRecord currentRecord = null;
    private Map<String, Object> deltaMap; // this variable is used for TsFileDb

    public TsFileQueryDataSet() {
        mapRet = new LinkedHashMap<>();
    }

    public void initForRecord() {
        size = mapRet.keySet().size();

        if (size > 0) {
            heap = new PriorityQueue<>(size);
            cols = new TsFileDynamicOneColumnData[size];
            deltaObjectIds = new String[size];
            measurementIds = new String[size];
            idxs = new int[size];
            timeMap = new HashMap<>();
        } else {
            LOG.error("TsFileQueryDataSet init row record occurs error! the size of ret is 0.");
            heap = new PriorityQueue<>();
        }

        int i = 0;
        for (String key : mapRet.keySet()) {
            cols[i] = mapRet.get(key);
            deltaObjectIds[i] = key.substring(0, key.lastIndexOf(PATH_SPLITTER));
            measurementIds[i] = key.substring(key.lastIndexOf(PATH_SPLITTER) + 1);
            idxs[i] = 0;

            if (cols[i] != null && (cols[i].valueLength > 0 || cols[i].timeLength > 0)) {
                heapPut(cols[i].getTime(0));
            }
            i++;
        }
    }

    protected void heapPut(long t) {
        if (!timeMap.containsKey(t)) {
            heap.add(t);
            timeMap.put(t, 1);
        }
    }

    protected Long heapGet() {
        Long t = heap.poll();
        timeMap.remove(t);
        return t;
    }

    public boolean hasNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }
        if (heap.peek() != null) {
            return true;
        }
        return false;
    }

    public RowRecord getNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        Long minTime = heapGet();
        if (minTime == null) {
            return null;
        }

        RowRecord r = new RowRecord(minTime, null, null);
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                r.setDeltaObjectId(deltaObjectIds[i]);
            }
            Field f = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);

            if (idxs[i] < cols[i].valueLength && minTime == cols[i].getTime(idxs[i])) {
                f.setNull(false);
                putValueToField(cols[i], idxs[i], f);
                idxs[i]++;
                if (idxs[i] < cols[i].valueLength) {
                    heapPut(cols[i].getTime(idxs[i]));
                }
            } else {
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
    }

    public boolean next() {
        if (hasNextRecord()) {
            currentRecord = getNextRecord();
            return true;
        }
        currentRecord = null;
        return false;
    }

    public RowRecord getCurrentRecord() {
        return currentRecord;
    }

    public void putValueToField(TsFileDynamicOneColumnData col, int idx, Field f) {
        switch (col.dataType) {
            case BOOLEAN:
                f.setBoolV(col.getBoolean(idx));
                break;
            case INT32:
                f.setIntV(col.getInt(idx));
                break;
            case INT64:
                f.setLongV(col.getLong(idx));
                break;
            case FLOAT:
                f.setFloatV(col.getFloat(idx));
                break;
            case DOUBLE:
                f.setDoubleV(col.getDouble(idx));
                break;
            case TEXT:
                f.setBinaryV(col.getBinary(idx));
                break;
            case ENUMS:
                f.setBinaryV(col.getBinary(idx));
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(col.dataType));
        }
    }

    public void clear() {
        this.ifInit = false;
        for (TsFileDynamicOneColumnData col : mapRet.values()) {
            col.clearData();
        }
    }
}