package cn.itcast.stormandkafka;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;


public class KafkaStormMain {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();

        //创建一个kafkaSpout用于我们从kafka当中读取数据
        KafkaSpoutConfig.Builder<String, String> kafkaSpoutBuilder = KafkaSpoutConfig.builder("node01:9092,node02:9092,node03:9092", "test");
        kafkaSpoutBuilder.setGroupId("hello");
        //有一个值可以设置从我们上次哪一个地方进行消费
        kafkaSpoutBuilder.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.LATEST);
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = kafkaSpoutBuilder.build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(kafkaSpoutConfig);//需要一个参数叫做kafkaSpoutConfig

        builder.setSpout("kafaSpout", kafkaSpout);
        builder.setBolt("myKafkaBolt", new MyKafkaBolt()).localOrShuffleGrouping("kafaSpout");

        Config config = new Config();

        if (args.length > 0) {
            config.setDebug(false);
            StormSubmitter submitter = new StormSubmitter();
            submitter.submitTopology(args[0], config, builder.createTopology());
        } else {
            config.setDebug(true);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("stormandkafka", config, builder.createTopology());
        }
    }
}