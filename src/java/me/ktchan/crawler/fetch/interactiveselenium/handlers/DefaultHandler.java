package me.ktchan.crawler.fetch.interactiveselenium.handlers;

import org.apache.nutch.protocol.interactiveselenium.handlers.InteractiveSeleniumHandler;
import org.openqa.selenium.By;
import org.openqa.selenium.WebDriver;

public class DefaultHandler implements InteractiveSeleniumHandler {
    public String processDriver(WebDriver driver) {
    	System.out.println("Hello Default InteractiveSeleniumHandler");
    	StringBuffer line =  new StringBuffer();
    	line.append(driver.findElement(By.tagName("body")).getAttribute("innerHTML"));
    	return line.toString();
    }

    public boolean shouldProcessURL(String URL) {
        return true;
    }
}
