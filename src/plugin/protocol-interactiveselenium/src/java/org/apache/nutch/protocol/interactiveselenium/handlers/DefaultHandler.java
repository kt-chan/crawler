/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.protocol.interactiveselenium.handlers;

import java.util.List;

import org.openqa.selenium.By;
import org.openqa.selenium.JavascriptExecutor;
import org.openqa.selenium.StaleElementReferenceException;
import org.openqa.selenium.TimeoutException;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedCondition;
import org.openqa.selenium.support.ui.WebDriverWait;

public class DefaultHandler implements InteractiveSeleniumHandler {
	public String processDriver(WebDriver driver) {
		System.out.println("Hello Default InteractiveSeleniumHandler");

		// Interactive Action
		driver.manage().window().maximize();
		scrollToBottom(driver);
		waitItemLoad(driver);
		
		StringBuffer line = new StringBuffer();
		line.append(driver.findElement(By.tagName("body")).getAttribute("innerHTML"));
		return line.toString();
	}

	public boolean shouldProcessURL(String URL) {
		return true;
	}

	private void scrollToBottom(WebDriver driver) {
		JavascriptExecutor jsx = (JavascriptExecutor) driver;
		long last_height = (long) jsx.executeScript("return document.body.scrollHeight");
		while (true) {
			jsx.executeScript("window.scrollTo(0, document.body.scrollHeight);");
			long new_height = (long) jsx.executeScript("return document.body.scrollHeight");

			if ((int) new_height == (int) last_height) {
				break;
			} else {
				last_height = new_height;
			}
		}
	}

	private void waitItemLoad(WebDriver driver) {

		try {

			(new WebDriverWait(driver, 60)).until(new ExpectedCondition<Boolean>() {
				public Boolean apply(WebDriver driver) {

					boolean allLoaded = true;
					int rounds = 0;
					while (true) {

						try {
							rounds++;
							allLoaded = true;

							List<WebElement> elements = driver.findElements(By.cssSelector("li[class*=\"item-sm\"]"));

							System.out.println("!!Loading item-sm(" + elements.size() + "): ");

							for (WebElement element : elements) {
								if(!element.getText().trim().isEmpty())
									System.out.print(element.getText().trim().replaceAll("\n", "") + "\t");
								allLoaded = allLoaded && !element.getText().isEmpty();
							}
							System.out.println();
							System.out.println("!!Loading item-sm(" + elements.size() + ") at round " + rounds);
							Thread.currentThread();
							Thread.sleep(1000);

							if (allLoaded || rounds > 10) {
								break;
							}

						} catch (StaleElementReferenceException e) {
							continue;
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					return allLoaded;

				}
			});
		} catch (TimeoutException e) {
			
		}

	}
}
