package webMagic.com.yunforge.spider.test;

import us.codecraft.webmagic.Page;
import us.codecraft.webmagic.Site;
import us.codecraft.webmagic.Spider;
import us.codecraft.webmagic.processor.PageProcessor;
import us.codecraft.webmagic.selector.Html;
import us.codecraft.webmagic.selector.Selectable;

public class GithubRepoPageProcessor implements PageProcessor {

	private Site site = Site.me().setRetryTimes(3).setSleepTime(100);

	@Override
	public void process(Page page) {
		// 当前下载的HTML页面的url
		String url = page.getRequest().getUrl();
		Html html = page.getHtml();
		Selectable selectable = html.$("section h3");
		String string = selectable.regex(">(.*)</h3>").get();
		page.putField("url", url);
		page.putField("string", string);
	}

	@Override
	public Site getSite() {
		return site;
	}

	public static void main(String[] args) {
		Spider.create(new GithubRepoPageProcessor())
				.addUrl("http://webmagic.io/docs/zh/posts/ch2-install/first-project.html")
				.addPipeline(new SpiderDataPieline()).thread(5).run();
	}
}