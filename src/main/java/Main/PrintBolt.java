package Main;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Map;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class PrintBolt extends BaseRichBolt {

    FileWriter fileWriter;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {
            File file=new File("/home/cordercorder/java/data.in");

            fileWriter=new FileWriter(file,true);
        }
        catch (Exception e){
            e.printStackTrace();
        }

        System.out.println("---------");
        System.out.println("okokokokok");
        System.out.println("---------");
    }

    public void execute(Tuple tuple) {
        String word=tuple.getStringByField("word");
        Integer count=tuple.getIntegerByField("count");
        try {
            fileWriter.write(word+" "+count+"\n");
            fileWriter.flush();
        }
        catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(word+"------>"+count);
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
