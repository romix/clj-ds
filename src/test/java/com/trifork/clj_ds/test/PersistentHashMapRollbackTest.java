/**
 * 
 */
package com.trifork.clj_ds.test;

import java.util.Vector;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.trifork.clj_ds.DeferredChangesMap;
import com.trifork.clj_ds.IPersistentMap;
import com.trifork.clj_ds.PersistentHashMap;


/**
 * @author eedrls
 *
 */
public class PersistentHashMapRollbackTest {
	static final int N = 20000;
	private static final long MAX_OPERATOPNS = 50000;
//	static final int N = 20;
//	private static final long MAX_OPERATOPNS = 200;
	private static final double READ_PROBABILITY = 0.85;
	private static final double WRITE_PROBABILITY = 0.93;
	private static final int MAX_ROUNDS = 50;
	private static final int MAX_STEPS_IN_UNTIL_SNAPSHOT = 100;
	private static final double FAILURE_PROBABILITY = 0.01;
	public String found;
	
	static class Command {
		final double probability;
		final int index;
		final String key;
		
		public Command(double probability, int index) {
			this.probability = probability;
			this.index = index;
			this.key = "a_" + index;
		}
		
		public String getKey() {
//			return "a_" + index;
			return key;
		}
	}

	static Vector<String> initialKeys = new Vector<String>(); 
	static Vector<Command> commands = new Vector<Command>(); 

	long reads = 0;
	long writes = 0;
	long removals = 0;
	private int snapshot_index;
	
	// If true, will check for maps equality, which is a rather time consuming operation.
	private boolean checkMapsEquality = true;
	

	@BeforeClass
	static public void setup() {
		
		for (int i=0;i<N;i++) {
			String key = "a_"+i;
			initialKeys.add(key);
		}
		
		for(long i=0; i<MAX_OPERATOPNS;++i) {
			double probability = Math.random();
			int index = (int) Math.ceil(2*(N+i)*Math.random());;
			commands.add(new Command(probability, index));
		}		
//		for(long i=0; i<MAX_OPERATOPNS;++i) {
//			double probability = Math.random();
//			double index = (int) Math.ceil(2*map.size()*Math.random());;
//			String key = "a_"+index;
//		}
	}
	

	@Test
	public final void testPersistentMap() throws Exception {
		
		IPersistentMap<String,String> pmap1 = PersistentHashMap.EMPTY;
		IPersistentMap<String,String> pmap2 = PersistentHashMap.EMPTY;
		
		// Create two maps with the same content
		for (String key: initialKeys) {
			pmap1 = pmap1.assoc(key, key);
			pmap2 = pmap2.assoc(key, key);
		}
		
		IPersistentMap<String, String> tmpMap1  = null;
		IPersistentMap<String, String> tmpMap2 = null;
		for(int i=0; i< MAX_ROUNDS; ++i) {
			// Perform series of operation on the map
			tmpMap1 = testPersistentMap(pmap1);
			// Perform series of operation on the map, allow for failures, use rollbacks
			// and redo steps
			tmpMap2 = testPersistentMapWithFailures(pmap2);
			// Check that the final results are the same
			Assert.assertEquals("Size should be the same", tmpMap1.count(), tmpMap2.count());
			if(checkMapsEquality)
				Assert.assertEquals("Maps should be the same", tmpMap1, tmpMap2);
			System.out.println("Size of map is: " + tmpMap1.count());
		}
		

	}

	
	@Test
	public final void testDeferredPersistentMap() throws Exception {
		
		IPersistentMap<String,String> pmap1 = PersistentHashMap.EMPTY;
		IPersistentMap<String,String> pmap2 = PersistentHashMap.EMPTY;
		
		// Create two maps with the same content
		for (String key: initialKeys) {
			pmap1 = pmap1.assoc(key, key);
			pmap2 = pmap2.assoc(key, key);
		}
		
		pmap1 = new DeferredChangesMap<String, String>(pmap1);
		pmap2 = new DeferredChangesMap<String, String>(pmap2);
		
		IPersistentMap<String, String> tmpMap1  = null;
		IPersistentMap<String, String> tmpMap2 = null;
		for(int i=0; i< MAX_ROUNDS; ++i) {
			// Perform series of operation on the map
			tmpMap1 = testPersistentMap(pmap1);
			// Perform series of operation on the map, allow for failures, use rollbacks
			// and redo steps
			tmpMap2 = testPersistentMapWithFailures(pmap2);
			// Check that the final results are the same
			Assert.assertEquals("Size should be the same at iteration " + i, tmpMap1.count(), tmpMap2.count());
			if(checkMapsEquality)
				Assert.assertEquals("Maps should be the same", tmpMap1, tmpMap2);
			System.out.println("Size of map is: " + tmpMap1.count());
		}

		System.out.println("Size of map is: " + tmpMap1.count());

	}

	static public IPersistentMap<String, String> oldMap;
	static public IPersistentMap<String, String> snapshot;
	
	/***
	 * Execute some commands on the persistent map.
	 * 
	 * @param map
	 * @throws Exception
	 */
	private IPersistentMap<String, String> testPersistentMap(IPersistentMap<String, String> map) throws Exception {
		for(Command command: commands) {
			String key = command.getKey();
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.valAt(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
//				boolean wasPresent = map.containsKey(key);
//				int oldCount = map.count();
				map = map.assoc(key,key);
//				if(wasPresent)
//					Assert.assertEquals("Count should be the same", oldCount, map.count());
//				else
//					Assert.assertTrue("Count should be different", oldCount < map.count());				
				writes++;
			} else {
//				boolean wasPresent = map.containsKey(key);
//				int oldCount = map.count();
				map = map.without(key);
//				if(!wasPresent)
//					Assert.assertEquals("Count should be the same", oldCount, map.count());
//				else {
//					Assert.assertTrue("Key should not be present", !map.containsKey(key));
//					Assert.assertTrue("Count should be different", oldCount > map.count());
//				}
				removals++;
			}
		}
		
		return map;
	}

	/***
	 * Execute some commands on the persistent map. Do snapshots every 100 changes.
	 * If something fails, rollback to the last snapshot and re-do steps.
	 * 
	 * @param map
	 * @throws Exception
	 */
	private IPersistentMap<String, String> testPersistentMapWithFailures(IPersistentMap<String, String> map) throws Exception {
		int i=0;
		while(i < commands.size()) {
			Command command = commands.elementAt(i);
			String key = command.getKey();
			
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.valAt(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
				map = map.assoc(key,key);
				writes++;
			} else {
				map = map.without(key);
				removals++;
			}
			
			if(Math.random() < FAILURE_PROBABILITY) {
//				System.out.println("Failure at index " + i);
//				System.out.println("Rollback to the state at index " + snapshot_index);
				// Perform a rollback (backtracking)
				i = snapshot_index+1;
				map = snapshot;
				// Now, let's re-do steps
				continue;
			}
			
			// Take a snapshot every X steps
			if(i%MAX_STEPS_IN_UNTIL_SNAPSHOT == 0) {
				snapshot = map;
				snapshot_index = i;
			}
			
			i++;
		}
		
		return map;
	}
}
