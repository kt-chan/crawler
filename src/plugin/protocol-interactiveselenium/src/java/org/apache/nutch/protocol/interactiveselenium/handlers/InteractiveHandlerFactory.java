package org.apache.nutch.protocol.interactiveselenium.handlers;

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.protocol.interactiveselenium.Http;

public final class InteractiveHandlerFactory {

	public static InteractiveSeleniumHandler getHandler(Configuration conf, String handlerName) {

		Http.LOG.info("loading " + handlerName + " ... ");
		InteractiveSeleniumHandler handler = null;
		try {
			String classToLoad = InteractiveSeleniumHandler.class.getPackage().getName() + "." + handlerName;
			handler = InteractiveSeleniumHandler.class
					.cast(Class.forName(classToLoad).getConstructor(Configuration.class).newInstance(conf));

		} catch (ClassNotFoundException e) {
			Http.LOG.info("Unable to load Handler class for: " + handlerName);
		} catch (InstantiationException e) {
			Http.LOG.info("Unable to instantiate Handler: " + handlerName);
		} catch (IllegalAccessException e) {
			Http.LOG.info("Illegal access with Handler: " + handlerName);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (handler == null) {
			Http.LOG.info("Handler Load Failed for " + handlerName);
		} else {
			Http.LOG.info("Successfully loaded handler for " + handlerName);
		}

		return handler;

	}

}
