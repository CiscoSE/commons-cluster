package com.cisco.commons.cluster.controller;

public interface PersistencyListener {

	/**
	 * Called when a map entry event received.
	 * @param key key
	 * @param oldValue old value
	 * @param newValue new value
	 * @param eventType event type
	 */
	void event(String key, String oldValue, String newValue, EventType eventType);
}
