package Main;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class PrintBolt extends BaseRichBolt {

    WriteData Writer;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        try {

            Writer=new WriteData("root","123456","jdbc:mysql://localhost:3306/storm?serverTimezone=UTC&useUnicode=true&characterEncoding=utf-8");
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
        long count=tuple.getLongByField("count");
        long id=tuple.getLongByField("id");
        try {
            Writer.InsertData(id,word,count);
        }
        catch (Exception e){
            e.printStackTrace();
        }
        System.out.println(id+"------>"+word+"------>"+count);
    }
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
