package com.trifork.clj_ds;

import java.util.Iterator;
import java.util.Map;
import java.util.Stack;

/***
 * This map remembers what kind of changes are to be applied to the 
 * original persistent tree. Once enough changes are accumulated and
 * functional handling of insertions and removals becomes inefficient, 
 * the changes are applied and a new more efficient representation is 
 * created.
 * 
 * The main aim is to reduce a cost of applying the change by avoiding
 * any "path copying", which is usually done by a standard PersistentHashMap.
 * Instead all changes are deferred.
 * 
 * We use a PIMPL approach, where the real implementation of the tree 
 * is hidden. Since we work with immutable tree, we can always replace
 * this tree representation with another representation, which may
 * be more efficient. 
 *  
 * @author eedrls
 *
 * @param <K>
 * @param <V>
 */
public class DeferredChangesMap<K,V> extends APersistentMap<K,V> 
	implements IEditableCollection<MapEntry<K, V>>{
	private static final long serialVersionUID = -3445334021123263289L;
	public static final int OP_ASSOC = 1;
	public static final int OP_WITHOUT = 2;
	public static final int NOP = 0;

	// Max number of accumulated changes
	private static final int MAX_CHANGES = 5;

	// Initially this is the original persistent tree.
	// But when changes are applied, it may refere to a new,
	// more efficient implementation.
	IPersistentMap<K,V> _map;
	
	// Change operation: assoc, without
	int _op;
	
	// Key that was updated
	protected K _key;
	
	protected V _value;
	
	protected int _count;

	public DeferredChangesMap(IPersistentMap<K,V> map) {
		_map = map;
		_op = NOP;
		_count = 0;
	}
	
	protected DeferredChangesMap(int op, K key, V value, DeferredChangesMap<K,V> map, int count) {
		this._op = op;
		this._key = key;
		this._value = value;
		this._map = map;
		this._count = count + 1;
		
		// Check if there are enough changes collected. 
		// If so, apply them.
		int numChanges = getNumChanges(this);
		if(numChanges > MAX_CHANGES) {
			applyChanges();
		}
	}
	
	@Override
	public IPersistentMap<K, V> assoc(K key, V val) {
		return new DeferredChangesMap<K, V>(OP_ASSOC, key, val, this, _count);
	}
	
	@Override
	public IPersistentMap<K, V> assocEx(K key, V val) throws Exception {
		if(_op != NOP)
			applyChanges();
		return assocEx(key, val);
	}

	@Override
	public IPersistentMap<K, V> without(K key) throws Exception {
		return new DeferredChangesMap<K, V>(OP_WITHOUT, key, null, this, _count);
	}
	@Override
	public Iterator<java.util.Map.Entry<K, V>> iteratorFrom(K key) {
		if(_op != NOP)
			applyChanges();
		return _map.iteratorFrom(key);
	}

	@Override
	public Iterator<java.util.Map.Entry<K, V>> reverseIterator() {
		if(_op != NOP)
			applyChanges();
		return _map.reverseIterator();
	}
	
	@Override
	public Iterator<java.util.Map.Entry<K, V>> iterator() {
		if(_op != NOP)
			applyChanges();
		return _map.iterator();
	}
	
	@Override
	public boolean containsKey(Object key) {
		if(_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return true;
		else if(_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return false;
		else 
			return _map.containsKey((K)key);
	}
	
	@Override
	public IMapEntry<K, V> entryAt(K key) {
		if(_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return new MapEntry<K, V>(_key, _value);
		else if(_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return null;
		else 
			return _map.entryAt(key);
	}

	@Override
	public int count() {
		if(_op == OP_ASSOC)
			return _map.count() + ((_map.containsKey(_key))?0:1);
		else if(_op == OP_WITHOUT)
			return _map.count() + ((_map.containsKey(_key))?-1:0);
		else 
			return _map.count();
	}

	@Override
	public IPersistentCollection<IMapEntry<K, V>> empty() {
		return _map.empty();
	}
	@Override
	public ISeq<IMapEntry<K, V>> seq() {
		if(_op != NOP)
			applyChanges();
		return _map.seq();
	}
	
	@Override
	public V valAt(K key) {
		if(_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return _value;
		if(_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return null;
		return (V)_map.valAt(key);
	}
	
	@Override
	public V valAt(K key, V notFound) {
		if(_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return _value;
		else if(_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return notFound;
		return (V)_map.valAt(key, notFound);
	}
	
	/***
	 * Apply a chain of deferred operations to the map.
	 * 
	 * TODO: Check if it is possible to apply multiple changes at once.
	 * May be use transients?
	 * 
	 * Also, some sort of reference counting can be useful, so that
	 * no copies/snapshots are generated for maps that are not used. 
	 */
	private void applyChanges2() {
		if(_op != NOP ) {
			// Create a more efficient represenation of _map
			if(_map instanceof DeferredChangesMap) {
				DeferredChangesMap<K,V> defMap = (DeferredChangesMap<K,V>)_map;
				defMap.applyChanges();
				_map = ((DeferredChangesMap<K,V>) _map)._map;
			} 
			
			if(_map instanceof PersistentHashMap){
				PersistentHashMap<K,V> perMap = (PersistentHashMap<K,V>)_map;
				if(_op == OP_ASSOC)
					_map = perMap.assoc(_key, _value);
				else if (_op == OP_WITHOUT) {
					try {
						_map = perMap.without(_key);
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}
			}
		_op = NOP;
		_key = null;
		_value = null;
		_count = 0;
		}
	}

	private void applyChanges() {
		if (_op != NOP) {

			// Find the root in the chain of changes
			Stack<IPersistentMap> chain = new Stack<IPersistentMap>();
			IPersistentMap curMap = _map;
			while (curMap != null && curMap instanceof DeferredChangesMap
					&& (((DeferredChangesMap) curMap)._op != NOP)) {
				chain.push(curMap);
				curMap = ((DeferredChangesMap) curMap)._map;
			}

			if (curMap != null) {
				// Make this map transient to apply all updates quickly
				// Apply all changes in one go
				IEditableCollection edMap = (IEditableCollection) curMap;
				ITransientMap transMap = (ITransientMap) edMap.asTransient();

				while (!chain.isEmpty()) {
					DeferredChangesMap changeMap = (DeferredChangesMap) chain
							.pop();
					if (changeMap._op == OP_ASSOC)
						transMap.assoc(changeMap._key, changeMap._value);
					else if (_op == OP_WITHOUT) {
						transMap.without(_key);
					}
				}

				// Make the data structure persistent again
				_map = transMap.persistentMap();
			}

			_op = NOP;
			_key = null;
			_value = null;
			_count = 0;
		}
	}

	private final int getNumChanges(DeferredChangesMap<K,V> map) {
		return map._count;
	}
	
	private final int getNumChanges2(DeferredChangesMap<K,V> map) {
		if(map._op == NOP)
			return 0;
		
		IPersistentMap curMap = map;
		int count = 0;
		while (curMap != null && curMap instanceof DeferredChangesMap
				&& (((DeferredChangesMap) curMap)._op != NOP)) {
			curMap = ((DeferredChangesMap) curMap)._map;
			count++;
		}

		return count;
		
	}

	private final int getNumChanges1(IPersistentMap<K,V> map) {
		if(map instanceof DeferredChangesMap) {
			DeferredChangesMap<K,V> defMap = (DeferredChangesMap<K,V>)map;
			if(_op == NOP)
				return 0;
			else
				return 1 + defMap.getNumChanges1(defMap._map);
		}
		else 
			return 0;
	}
	
	@Override
	public void putAll(Map m) {
		throw new UnsupportedOperationException();
	}

	@Override
	public ITransientCollection<MapEntry<K, V>> asTransient() {
		if(_op != NOP)
			applyChanges();
		return ((IEditableCollection)_map).asTransient();
	}
}
