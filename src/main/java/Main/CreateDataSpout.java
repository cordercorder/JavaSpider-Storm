package Main;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: 金任任
 * @Class: 计科1604
 * @Number: 2016014537
 */
public class CreateDataSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private String url;

    private String language_regex,id_regex;

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector=spoutOutputCollector;
        url=new String("https://codeforc.es/problemset/status");
        language_regex=new String("<td(\\s*)(class=\"dark\")?>(([a-z]*[A-Z]*\\s*[0-9]*[#+]*)*)</td>");
        id_regex=new String("<tr(\\s*)(class=\"last-row\")?(\\s*)?data-submission-id=\"(\\d+)\"");
    }

    public void nextTuple() {
        String language=Spider(language_regex,3);
        String id=Spider(id_regex,4);
        System.out.println("线程名: "+Thread.currentThread().getName()+" "+language);
        System.out.println("线程名: "+Thread.currentThread().getName()+" "+id);
        collector.emit(new Values(language,id));
        Utils.sleep(10000);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("language","id"));
    }


    public String Spider(String pat,int pos){
        BufferedReader reader=null;
        String result=new String();
        try{
            URL realUrl=new URL(url);
            URLConnection connection=realUrl.openConnection();
            connection.connect();
            reader=new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String s;
            while((s=reader.readLine())!=null){
                result+=s;
            }

        }
        catch (Exception e1){
            e1.printStackTrace();
        }
        finally {
            try{
                reader.close();
            }
            catch (Exception e2){
                e2.printStackTrace();
            }
        }
        return RegexString(result,pat,pos);
    }

    public String RegexString(String source,String pat,int pos){

        Pattern pattern=Pattern.compile(pat);

        Matcher matcher=pattern.matcher(source);

        String tmp,ans=new String();
        int L,R;

        int sum=0;

        while(matcher.find()){
            tmp=matcher.group(pos);
            L=0;
            R=tmp.length()-1;
            while(L<tmp.length()&&tmp.charAt(L)==' '){
                L++;
            }
            while(R>=0&&tmp.charAt(R)==' '){
                R--;
            }
            if(L<R+1){
                ans+=tmp.substring(L,R+1)+"$";
                sum++;
            }
        }

        System.out.println(sum);
        return ans;
    }


}
