package com.imooc.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentTest {

    @Test
    public void testReadFile()throws Exception{
        String path="/Users/binbin/data/100_test.log";
        BufferedReader bufferedReader=new BufferedReader(
                new InputStreamReader(new FileInputStream(new File(path)))
        );
        UserAgentParser userAgentParser = new UserAgentParser();
        String line="";
        int count=0;

        Map<String,Integer> browserMap=new HashMap<String, Integer>();

        while (line!=null){
            line=bufferedReader.readLine();//一次读入一行
            count++;
            if(StringUtils.isNotBlank(line)){
                String source=line.substring(getCharacterPosition(line,"\"",5)+1);
                UserAgent agent = userAgentParser.parse(source);
                String browser = agent.getBrowser();
                String engine = agent.getEngine();
                String engineVersion = agent.getEngineVersion();
                String os = agent.getOs();
                String platform = agent.getPlatform();
                boolean isMobile = agent.isMobile();

                Integer browserValue=browserMap.get(browser);
                if(browserValue!=null){
                    browserMap.put(browser,browserValue+1);
                }else {
                    browserMap.put(browser,1);
                }

                System.out.println("浏览器：" + browser+""+"引擎：" + engine+""+"引擎版本：" + engineVersion+""+"操作系统：" + os+""+"平台：" + platform+""+"是否是移动设备：" + isMobile);

            }
        }

        System.out.println(count);

        for(Map.Entry<String,Integer> entry:browserMap.entrySet()){
            System.out.println(entry.getKey()+":"+ entry.getValue());
        }
    }

    @Test
    public void testMatcher(){
//        String value="116.24.66.215 - - [25/Oct/2018:12:13:40 +0000] \"GET /api/orca/schedule/sketch/sketch?bp_id=f866e36a-d84d-11e8-8b36-fa163ed0a316 HTTP/1.1\" 200 1014 \"http://factory.dev.cloudtogo.cn/project/instance?id=FP1810251206376560000015438217\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36\" \"-\" [0.026 0.026] ";
        String value="116.24.66.215 - - [25/Oct/2018:12:13:39 +0000] \"GET /api/factory/project/instance/params/e5db875f3b624b11b31ec58c065f49ea HTTP/1.1\" 200 2406 \"http://factory.dev.cloudtogo.cn/project/instance?id=FP1810251206376560000015438217\" \"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36\" \"-\" [0.008 0.008] ";
        int index=getCharacterPosition(value,"\"",5);
        System.out.println(index);

    }

    /**
     * 获取指定字符串中指定标识符的字符串出现的索引的位置
     * @param value
     * @param operator
     * @param index
     * @return
     */
    private int getCharacterPosition(String value,String operator,int index){


        Matcher matcher= Pattern.compile(operator).matcher(value);
        int mIdx=0;
        while (matcher.find()){
            mIdx++;

            if(mIdx==index){
                break;
            }
        }
        return matcher.start();
    }

    /**
     * 单元测试：UserAgent工具类的使用
     */
    @Test
    public void testUserAgentPaser() {

        String source = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36";
        UserAgentParser userAgentParser = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean isMobile = agent.isMobile();
        System.out.println("浏览器：" + browser+""+"引擎：" + engine+""+"引擎版本：" + engineVersion+""+"操作系统：" + os+""+"平台：" + platform+""+"是否是移动设备：" + isMobile);

    }
}
