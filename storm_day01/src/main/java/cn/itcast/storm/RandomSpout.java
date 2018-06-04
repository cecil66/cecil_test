package cn.itcast.storm;


import jdk.nashorn.internal.objects.NativeRangeError;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;


public class RandomSpout extends BaseRichSpout {

    private  String[]  arrays ;
    private SpoutOutputCollector collector;
    private Random random;

    /**
     * 初始化的方法，只会被执行一次，所有关于初始化的配置都可以放在这里面来做
     * @param conf
     * @param context
     * @param collector
     */
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        random = new Random();
        this.collector = collector;
        arrays = new String[]{"hadoop hive","hive flume","haodop sqoop","hive  flume","kafka hadoop","kafka  storm"};
    }

    /**
     * nextTuple   这个方法会被反复不断地调用，只要有了数据，就会被调用
     */
    @Override
    public void nextTuple() {
        try {
            //通过collector调用emit方法来进行数据的方法
            String words =  arrays[random.nextInt(arrays.length)];
            collector.emit(new Values(words));
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 发送出去的一条条数给定义一个字段，map  key  value
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words"));
    }
}
