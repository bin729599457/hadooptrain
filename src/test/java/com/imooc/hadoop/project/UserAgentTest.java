package com.imooc.hadoop.project;

import com.kumkee.userAgent.UserAgent;
import com.kumkee.userAgent.UserAgentParser;

public class UserAgentTest {

    public static void main(String[] args) {

        String source="Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36";
        UserAgentParser userAgentParser  = new UserAgentParser();
        UserAgent agent = userAgentParser.parse(source);

        String browser = agent.getBrowser();
        String engine = agent.getEngine();
        String engineVersion = agent.getEngineVersion();
        String os = agent.getOs();
        String platform = agent.getPlatform();
        boolean isMobile = agent.isMobile();

        System.out.println("浏览器：" + browser);
        System.out.println("引擎：" + engine);
        System.out.println("引擎版本：" + engineVersion);
        System.out.println("操作系统：" + os);
        System.out.println("平台：" + platform);
        System.out.println("是否是移动设备：" + isMobile);

    }
}
