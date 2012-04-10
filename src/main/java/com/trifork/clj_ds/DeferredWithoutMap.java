package com.trifork.clj_ds;

final public class DeferredWithoutMap<K, V> extends DeferredChangesMap<K, V> {
	public DeferredWithoutMap(int op, K key, DeferredChangesMap<K, V> map) {
		super(op, key, null, map, map._count);
	}

	@Override
	public IMapEntry<K, V> entryAt(K key) {
		if (_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return null;
		return super.entryAt(key);
	}

	@Override
	public V valAt(K key) {
		if (_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return null;
		return (V) super.valAt(key);
	}

	@Override
	public V valAt(K key, V notFound) {
		if (_op == OP_WITHOUT && (key == _key || key.equals(_key)))
			return notFound;
		return (V) super.valAt(key, notFound);
	}

	public void applyOp() {
		if (_map instanceof PersistentHashMap) {
			PersistentHashMap<K, V> perMap = (PersistentHashMap<K, V>) _map;
			try {
				_map = perMap.without(_key);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public int count() {
		if(_op == OP_WITHOUT) 
			return _map.count() + ((_map.containsKey(_key)) ? -1 : 0);
		return super.count();
	}

	@Override
	public boolean containsKey(Object key) {
		if(_op == OP_WITHOUT && (_key == _key || key.equals(_key)))
			return false;
		return super.containsKey((K)key);
	}
}
