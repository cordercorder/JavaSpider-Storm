package Main;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class CountBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Map<String,Integer> cnt=new HashMap<String,Integer>();

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
    }

    public void execute(Tuple tuple) {
        String word=tuple.getStringByField("word");
        int count;
        if(cnt.get(word)!=null){
            count=cnt.get(word)+1;
            cnt.put(word,count);
        }
        else{
            count=1;
            cnt.put(word,count);
        }
        collector.emit(new Values(word,count));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word","count"));
    }
}
