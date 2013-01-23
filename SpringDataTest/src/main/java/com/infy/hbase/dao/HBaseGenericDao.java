package com.infy.hbase.dao;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;

import com.infy.hbase.entity.AbstractHBaseEntity;


public interface HBaseGenericDao<T extends AbstractHBaseEntity, RK extends Object> extends HBaseDao
{

    boolean create(T newInstance);

    T read(RK id);
    
    List<T> readAll();

    void update(T transientObject);
    
    void updateCollection(Collection<T> o);

    void delete(T persistentObject);
    
    void deleteCollection(Collection<T> o);
    
}
