package com.atguigu.partiton;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PartitonReducer extends Reducer<Text,PartitionBean, Text, PartitionBean> {
    private PartitionBean partitionBean = new PartitionBean();
    private long upFlow;
    private long downFlow;
    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {
        for (PartitionBean value: values){
            upFlow+= value.getUpFlow();
            downFlow += value.getDownFlow();
        }
        this.partitionBean.set(upFlow,downFlow);
        context.write(key, this.partitionBean);
    }
}
