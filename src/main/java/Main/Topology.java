package Main;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */

public class Topology{

    public static void main(String[] args){
        TopologyBuilder builder=new TopologyBuilder();

        builder.setSpout("DataSpout",new CreateDataSpout());

        builder.setBolt("HandleBolt",new HandleBolt(),1).shuffleGrouping("DataSpout");

        builder.setBolt("CountBolt",new CountBolt(),1).fieldsGrouping("HandleBolt",new Fields("word"));

        builder.setBolt("PrintBolt",new PrintBolt(),1).globalGrouping("CountBolt");

        Config config=new Config();

        try{
            if(args==null||args.length==0){
                LocalCluster cluster=new LocalCluster();
                cluster.submitTopology("stormTopology",config,builder.createTopology());
            }
            else{
                config.setNumWorkers(1);
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}
