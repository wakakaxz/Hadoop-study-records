package com.xz.mapreduce.writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xz
 * 1. 实现 WritableComparable 接口
 * 2. 重写序列化和反序列化方法
 * 3. 重写空参构造
 * 4. toString 方法
 * 5. compareTo 比较方法
 */
public class FlowBean implements WritableComparable<FlowBean> {
    private long upFlow; // 上行流量
    private long downFlow;// 下行流量
    private long sumFlow;// 总流量

    // 空参构造
    public FlowBean() {

    }

    @Override
    public int compareTo(FlowBean flowBean) {
        return this.sumFlow > flowBean.sumFlow ? -1 : 1;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        // 和序列化顺序一致
        this.upFlow = dataInput.readLong();
        this.downFlow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    /**
     * 直接相加不需要参数
     */
    public void setSumFlow() {
        this.sumFlow = this.upFlow + this.downFlow;
    }
}
