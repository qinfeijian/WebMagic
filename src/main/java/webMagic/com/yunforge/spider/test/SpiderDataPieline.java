package webMagic.com.yunforge.spider.test;

import us.codecraft.webmagic.ResultItems;
import us.codecraft.webmagic.Task;
import us.codecraft.webmagic.pipeline.Pipeline;

public class SpiderDataPieline implements Pipeline {

	@Override
	public void process(ResultItems resultItems, Task task) {
		// TODO Auto-generated method stub
		String url = resultItems.get("url").toString();
		String string = resultItems.get("string").toString();
		System.out.println("pipeline ");
		System.out.println(url);
		System.out.println(string);
		

	}

}
