package cn.itcast.storm;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {

    private Map<String,Integer> map;

    /*
   覆写我们的prepare方法，用于我们的初始化
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        map = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String word = input.getStringByField("word");
        Integer num = input.getIntegerByField("num");
        if(map.containsKey(word)){
            map.put(word,map.get(word)+num);

        }else{
            map.put(word,num);
        }
        System.out.println(map.toString());

    }

    //如果下游没有bolt，数据就不用往下发送了，也不用管这个方法了
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}

