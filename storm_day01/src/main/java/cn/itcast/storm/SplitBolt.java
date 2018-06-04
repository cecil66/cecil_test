package cn.itcast.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt extends BaseBasicBolt {

    /**
     * execute  表示我们被执行的方法，每一条数据过来都会调用这个execut方法
     * @param input
     * @param collector
     * tuple  就是我们的一条条的数据从上游发送过来的
     * BasicOutputCollector  通过调用emit方法来实现我们的数据往下游发送
     */
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String words = input.getStringByField("words");
        //将我们上游发送的数据切割开来，变成一个个的单词
        String[] split = words.split(" ");  //  hadoop 1   hive  1
        for (String str : split) {
            collector.emit(new Values(str,1));
        }



    }
    /**
     * 发送出去的一条条数给定义一个字段，map  key  value
     * @param declarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","num"));
    }
}