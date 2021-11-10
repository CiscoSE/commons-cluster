package com.cisco.commons.cluster.controller;


import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public interface ClusterService {

	public void init();
	
    public String get(String key) throws Exception;

    public String put(String key, String value) throws Exception;
    
    /**
     * Put with advisory time to live.
     * @param key
     * @param value
     * @param ttlSeconds
     * @return
     * @throws Exception
     */
    public String put(String key, String value, long ttlSeconds)
		throws Exception;
    
    /**
     * Keep alive for a key.
     * Can be used only from same client connection which created it with ttl.
     * @param key
     * @throws IOException
     * @throws Exception 
     */
    public void keepAlive(String key) throws IOException, Exception;

    public String remove(String key) throws Exception;
    
    public void addListener(PersistencyListener persistencyListener, String prefix);

    public Collection<String> getAllKeys(String prefix) throws Exception;

    public Map<String, String> getAllKeyValues(String prefix) throws Exception;
    
    public void deleteAllKVs(String prefix) throws Exception;

    public void close();

}
