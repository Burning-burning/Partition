package com.atguigu.partiton;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text,Text, PartitionBean> {
    private Text phoneNumber = new Text();
    private PartitionBean partitionBean = new PartitionBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String phone = fields[1];

        long upFlow = Long.parseLong(fields[fields.length-3]);
        long downFlow = Long.parseLong(fields[fields.length-2]);
        this.phoneNumber.set(phone);
        this.partitionBean.set(upFlow,downFlow);

        context.write(phoneNumber,partitionBean);
    }
}
