package com.trifork.clj_ds;

final public class DeferredAssocMap<K, V> extends DeferredChangesMap<K, V> {

	// New value (in case of an assoc operation)
	V _value;

	public DeferredAssocMap(int op, K key, V value, DeferredChangesMap<K, V> map) {
		super(op, key, value, map, map._count);
		_value = value;
	}

	@Override
	public IMapEntry<K, V> entryAt(K key) {
		if (_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return new MapEntry<K, V>(_key, _value);
		return super.entryAt(key);
	}

	@Override
	public V valAt(K key) {
		if (_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return _value;
		return (V) super.valAt(key);
	}

	@Override
	public V valAt(K key, V notFound) {
		if (_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return _value;
		return (V) super.valAt(key, notFound);
	}

	public void applyOp() {
		if (_map instanceof PersistentHashMap) {
			PersistentHashMap<K, V> perMap = (PersistentHashMap<K, V>) _map;
			_map = perMap.assoc(_key, _value);
		}
	}

	@Override
	public int count() {
		if(_op == OP_ASSOC)
			return _map.count() + ((_map.containsKey(_key)) ? 0 : 1);
		return super.count();
	}
	
	@Override
	public boolean containsKey(Object key) {
		if(_op == OP_ASSOC && (key == _key || key.equals(_key)))
			return true;
		return super.containsKey((K)key);
	}
}
