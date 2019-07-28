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
public class HandleBolt extends BaseRichBolt {

    private OutputCollector collector;

    private Map<String,Boolean> isv;

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        isv=new HashMap<String, Boolean>();
    }

    public void execute(Tuple tuple) {
        String language=tuple.getStringByField("language");
        String id=tuple.getStringByField("id");

        String[] arr_lan=language.split("\\$");
        String[] arr_id=id.split("\\$");
        for(int i=0;i<arr_lan.length;i++){
            if(isv.get(arr_id[i])==null){
                isv.put(arr_id[i],true);
                collector.emit(new Values(arr_id[i],arr_lan[i]));
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id","word"));
    }
}
