package cn.itcast.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class MainTopology {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder builder = new TopologyBuilder();
        //设置我们topology的spout
        //这里还可以变成三个参数的，最后一个参数指定我们程序运行的线程数量
        builder.setSpout("randomSpout",new RandomSpout(),1);
        //通过shuffleGrouping这个方法，将我们的spout与bolt之间的前后顺序关系给指定好
        //localOrShuffleGrouping  优先处理本地的数据，本地的数据不需要网络的拷贝，速度会更加快，如果本地没有数据了，再通过网络拷贝去其他节点上拉取数据来进行处理
        builder.setBolt("splitBolt",new SplitBolt(),3).globalGrouping("randomSpout");//.shuffleGrouping("randomSpout");//fieldsGrouping("randomSpout",new Fields("words"))//
        builder.setBolt("countBolt",new CountBolt(),3).globalGrouping("splitBolt");
        //提交topology的时候一些配置信息
        Config config = new Config();
        if(args.length > 0){
            config.setNumWorkers(3);
            //关闭日志信息，提交到集群上面去不需要大量的日志出来
            config.setDebug(false);
            //集群模式运行
            StormSubmitter submitter = new StormSubmitter();
            //通过topologyBuilder来创建我们的topology
            StormTopology topology = builder.createTopology();
            submitter.submitTopology(args[0],config,topology);
        }else{
            //打开调试模式，我们本地运行可以看到更多的日志信息
            config.setDebug(true);
            //本地模式运行
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("localStrom",config,builder.createTopology());
        }








    }


}
