package cn.itcast.stormandkafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.List;

public class MyKafkaBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        Object value = input.getValue(4);

        System.out.println("我要准备打印了");
        System.out.println(value.toString()+"是我接收到的值");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
