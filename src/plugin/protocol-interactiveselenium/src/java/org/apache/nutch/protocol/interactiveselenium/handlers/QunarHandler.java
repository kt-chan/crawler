package org.apache.nutch.protocol.interactiveselenium.handlers;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.httpclient.Http;
import org.openqa.selenium.By;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.interactions.Actions;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

public class QunarHandler extends DefaultHandler implements InteractiveSeleniumHandler {

	private int timeout;
	private String cssSelectorSalesItem = "li[class*=\"item-sm\"]";
	private String cssSelectorNaviBar = "div[class*=\"m-sidenav-sub\"]";

	public QunarHandler(Configuration conf) {
		super(conf);
		timeout = Integer.parseInt(conf.get("interactiveselenium.handler.waittimeout", "10"));
	}

	public String processDriver(WebDriver driver) {

		boolean pass = false;
		StringBuffer out = new StringBuffer();

		 try {
		
		 // Interactive Action with timeout
		
		 WebDriverWait webDriverWait = new WebDriverWait(driver, timeout);
		 webDriverWait.until(new ExpectedCondition<Boolean>() {
		 public Boolean apply(WebDriver driver) {
		
			 scrollToBottom(driver);
			 waitItemLoad(driver, By.cssSelector(cssSelectorSalesItem));
			 // clickItems(driver, By.cssSelector(cssSelectorNaviBar));
			 return false;
		 }
		 });
		
		 } catch (TimeoutException e) {
		 // just log this exception
		 Http.LOG.info(this.getClass().getName() + " has throw TimeoutException: " + e.getMessage());
		 }

		pass = this.getPageSource(driver, out);
//		if (pass) {
//			DebugWriterUtil.delete("./debug.log");
//			DebugWriterUtil.write(out.toString(), "./debug.log");
//		}
		return out.toString();
	}

	public boolean getPageSource(WebDriver driver, StringBuffer out) {
		int pre = out.length();
		out.append(driver.getPageSource());
		return (out.length() > pre);
	}

	public boolean shouldProcessURL(String URL) {
		return URL.toLowerCase().contains("qunar.com");
	}

	private boolean clickItems(WebDriver driver, By cssSelector) {

		boolean loaded = true;
		int rounds = 0;
		while (true) {
			try {
				rounds++;
				loaded = true;

				List<WebElement> elements = driver.findElements(cssSelector);
				for (WebElement element : elements) {
					Actions actions = new Actions(driver);
					actions.moveToElement(element);
				}
				Thread.currentThread();
				Thread.sleep(1000);

				if (loaded || rounds > 10) {
					break;
				}

			} catch (StaleElementReferenceException e) {
				continue;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return loaded;

	}

	private boolean clickItem(WebDriver driver, WebElement webElement) {
		return false;
	}

	private boolean waitItemLoad(WebDriver driver, By cssSelector) throws TimeoutException {

		boolean loaded = true;
		int rounds = 0;
		while (true) {

			try {
				rounds++;
				loaded = true;

				List<WebElement> elements = driver.findElements(cssSelector);

				for (WebElement element : elements) {
					if (!element.getText().trim().isEmpty())
						loaded = loaded && !element.getText().isEmpty();
				}
				Thread.currentThread();
				Thread.sleep(1000);

				if (loaded || rounds > 10) {
					break;
				}

			} catch (StaleElementReferenceException e) {
				continue;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return loaded;

	}
}
