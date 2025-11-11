package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 * @tparam T 缓存的对象类型
 * 我们的缓存数据结构包括三个哈希表:
 * 1. cache: 存储实际的缓存数据，键是资源的唯一标识符，值是缓存的对象
 * 2. references: 存储每个资源的引用计数，键是资源的唯一标识符，值是该资源的引用个数
 * 3. getting: 存储正在被获取的资源，键是资源的唯一标识符，值是一个布尔值，表示该资源正在被获取
 * 缓存的主要操作包括:
 * 1. get(key): 获取一个缓存资源，如果资源在缓存中，直接返回并增加引用计数；如果资源不在缓存中，调用子类实现的获取方法获取资源，并将其加入缓存，引用计数设为1
 * 2. release(key): 释放一个缓存资源，引用计数减1，如果引用计数为0，则将资源写回并从缓存中移除
 * 3. close(): 关闭缓存，写回所有资源并清空缓存
 *
 * 以后如果要实现具体的缓存，只需要继承AbstractCache类，并实现getForCache和releaseForCache方法即可
 * 这样就可以实现不同类型的缓存，例如页面缓存、记录缓存等
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                     // 实际缓存的数据
    private HashMap<Long, Integer> references;          // 元素的引用个数
    private HashMap<Long, Boolean> getting;             // 正在获取某资源的线程

    private int maxResource;                            // 缓存的最大缓存资源数
    private int count = 0;                              // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 获取一个缓存,引用计数加1
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            // 检查是否有其他线程正在获取该资源
            if(getting.containsKey(key)) {
                // 请求的资源正在被其他线程获取,等待
                lock.unlock();
                try {
                    // 已经有thread在获取该资源,所以先休息一下(1s)，避免忙等待
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                // 引用计数加1
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                // 缓存已满，抛出异常
                throw Error.CacheFullException;
            }

            count ++;
            // 这表明资源正在被获取
            getting.put(key, true);
            lock.unlock();
            break;
        }

        // 资源不在缓存中，调用子类实现的获取方法获取资源
        T obj = null;
        try {
            // 这里相当于sql从磁盘加载数据到内存
            obj = getForCache(key);
        } catch(Exception e) {
            lock.lock();
            count --;
            // 资源获取失败，移除正在获取的标记
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        // 资源获取成功，加入缓存，引用计数设为1
        getting.remove(key);
        // 把这个新获取的资源放入缓存
        cache.put(key, obj);
        // 该资源的引用计数设为1
        references.put(key, 1);
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存
     * 引用计数减1，若引用计数为0，则将资源写回并从缓存中移除
     * @param key 资源的唯一标识符
     */
    protected void release(long key) {
        lock.lock();
        try {
            // 引用计数减1
            int ref = references.get(key) - 1;
            // 若引用计数为0，则将资源写回并从缓存中移除
            if(ref == 0) {
                T obj = cache.get(key);
                // 调用子类实现的写回方法
                releaseForCache(obj);
                // 释放该资源
                references.remove(key);
                // 从缓存中移除该资源
                cache.remove(key);
                // 缓存中元素个数减1
                count --;
            } else {
                // 更新引用计数
                references.put(key, ref);
            }
        } finally {
            // 释放锁
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     * 我们把缓存中的所有资源都写回磁盘，并清空缓存
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            // 遍历所有缓存的资源
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
