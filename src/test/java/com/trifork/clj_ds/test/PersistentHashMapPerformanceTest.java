/**
 * 
 */
package com.trifork.clj_ds.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import org.junit.BeforeClass;
import org.junit.Test;

import EDU.oswego.cs.dl.util.concurrent.ConcurrentHashMap;

import com.trifork.clj_ds.DeferredChangesMap;
import com.trifork.clj_ds.IPersistentMap;
import com.trifork.clj_ds.ITransientMap;
import com.trifork.clj_ds.PersistentHATTrie;
import com.trifork.clj_ds.PersistentHashMap;
import com.trifork.clj_ds.PersistentTreeMap;
import com.trifork.clj_ds.PersistentMap;


/**
 * @author eedrls
 *
 */
public class PersistentHashMapPerformanceTest {
	static final int N = 2000;
	private static final long MAX_OPERATOPNS = 500000;
//	private static final long MAX_OPERATOPNS = 10;
//	private static final double READ_PROBABILITY = 0.85;
//	private static final double WRITE_PROBABILITY = 0.93;
	private static final double READ_PROBABILITY = 0.35;
	private static final double WRITE_PROBABILITY = 0.93;
	private static final int MAX_ROUNDS = 50;
	public String found;
	Map<String, String> map = new HashMap<String, String>();
	@SuppressWarnings("unchecked")
	Map<String, String> cmap = new ConcurrentHashMap();
	PersistentHashMap<String,String> pmap = PersistentHashMap.create(map);
	
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
	
	/**
	 * Test method for {@link com.trifork.clj_ds.PersistentHashMap#create(java.util.Map)}.
	 */
	@Test
	public final void testJavaMap() {
		
		for (String key: initialKeys) {
			map.put(key, key);
		}

		for(int i=0; i< MAX_ROUNDS; ++i)
			testJavaMap(map);

		System.out.println("Size:" + map.size());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	@Test
	public final void testJavaConcurrentMap() {
		
		for (String key: initialKeys) {
			cmap.put(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			testJavaConcurrentMap(cmap);

		System.out.println("Size:" + cmap.size());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	@Test
	public final void testPersistentMap() throws Exception {
		
		PersistentMap<String,String> pmap = PersistentMap.empty();
		
		for (String key: initialKeys) {
			pmap = pmap.with(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			pmap = checkPersistentMap(pmap);

//		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}
	
	@Test
	public final void testClojurePersistentMap() throws Exception {
		
		IPersistentMap<String,String> pmap = PersistentHashMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			pmap = checkClojurePersistentMap(pmap);

		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	@Test
	public final void testClojurePersistentTreeMap() throws Exception {
		
		IPersistentMap<String,String> pmap = PersistentTreeMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			pmap = checkClojurePersistentMap(pmap);

		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	
	@Test
	public final void testTransientPersistentMap() throws Exception {
		
		ITransientMap<String, String>  tmap = PersistentHashMap.EMPTY.asTransient();
		
		for (String key: initialKeys) {
			tmap = tmap.assoc(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			tmap = checkClojureTransientPersistentMap(tmap);

		IPersistentMap<String, String> pmap = tmap.persistentMap();
		
		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	@Test
	public final void testPersistentMapWithoutChangingMap() throws Exception {
		
		IPersistentMap<String,String> pmap = PersistentHashMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			pmap = checkClojurePersistentMapWithoutChangingMap(pmap);

		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}

	@Test
	public final void testDeferredPersistentMap() throws Exception {
		
		IPersistentMap<String,String> pmap = PersistentHashMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}
		
		// Make a deferred map out of it.
		pmap = new DeferredChangesMap<String, String>(pmap);
		
		for(int i=0; i< MAX_ROUNDS; ++i)
			pmap = checkClojurePersistentMap(pmap);

		System.out.println("Size:" + pmap.count());
		System.out.println("Reads:" + reads);
		System.out.println("Writes:" + writes);
		System.out.println("Removals:" + removals);
	}
	
	private void testJavaMap(Map<String, String> map) {
		for(Command command: commands) {
			String key = command.getKey();
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.get(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
				map.put(key,key);
				writes++;
			} else {
				map.remove(key);
				removals++;
			}
		}
	}
	
	private void testJavaConcurrentMap(Map<String, String> map) {
		for(Command command: commands) {
			String key = command.getKey();
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.get(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
				map.put(key,key);
				writes++;
			} else {
				map.remove(key);
				removals++;
			}
		}
	}
	
	private PersistentMap<String, String> checkPersistentMap(PersistentMap<String, String> map) throws Exception {
		int i=0;
		for(Command command: commands) {
			String key = command.getKey();
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.get(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
				map = map.with(key,key);
				writes++;
			} else {
				map = map.without(key);
				removals++;
			}
			
			++i;
		}
		
		return map;	
	}
	
	static public IPersistentMap<String, String> oldMap;
	
	private IPersistentMap<String, String> checkClojurePersistentMap(IPersistentMap<String, String> map) throws Exception {
		int i=0;
		for(Command command: commands) {
			oldMap = map;
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
			
			++i;
		}
		
		return map;	
	}


	private ITransientMap<String, String> checkClojureTransientPersistentMap(ITransientMap<String, String> map) throws Exception {
		int i=0;
		for(Command command: commands) {
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
			
			++i;
		}
		
		return map;	
	}

	private IPersistentMap<String, String> checkClojurePersistentMapWithoutChangingMap(IPersistentMap<String, String> map) throws Exception {
		for(Command command: commands) {
			oldMap = map;
			String key = command.getKey();
			if(command.probability < READ_PROBABILITY) {
				// Read from the map
				found = map.valAt(key);
				reads++;
			} else if(command.probability > READ_PROBABILITY && command.probability < WRITE_PROBABILITY) {
				// Write to the map
				map.assoc(key,key);
				writes++;
			} else {
				map.without(key);
				removals++;
			}
		}
		
		return map;	
	}

	@Test
	public  void testSmallUpdatesOnBigClojurePersistentMap() throws Exception {
		IPersistentMap<String,String> pmap = PersistentHashMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}

		checkSmallUpdatesOnBigClojurePersistentMap(pmap);
	}
	
	@Test
	public  void testSmallUpdatesOnBigDefferedClojurePersistentMap() throws Exception {
		IPersistentMap<String,String> pmap = PersistentHashMap.EMPTY;
		
		for (String key: initialKeys) {
			pmap = pmap.assoc(key, key);
		}

		checkSmallUpdatesOnBigClojurePersistentMap(new DeferredChangesMap<String, String>(pmap));
	}


	private IPersistentMap<String, String> checkSmallUpdatesOnBigClojurePersistentMap(IPersistentMap<String, String> map) throws Exception {
		String oldKey = "a_" + 100;
		for(int i=0; i<1000000; ++i) {
			IPersistentMap<String, String> newMap = map.assoc("newKey", "newVal");
			String value = newMap.valAt("newKey");
			value = newMap.valAt(oldKey);
		}
		return map;	
	}
}
