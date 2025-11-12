package top.guoziyang.mydb.backend.vm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * 维护了一个依赖等待图，以进行死锁检测.
 * 这里比较像OS中的死锁检测问题.
 */
public class LockTable {
    
    private Map<Long, List<Long>> x2u;  // 某个XID已经获得的资源的UID列表
    private Map<Long, Long> u2x;        // UID被某个XID持有

    private Map<Long, List<Long>> wait; // 正在等待UID的XID列表
    private Map<Long, Lock> waitLock;   // 正在等待资源的XID的锁
    private Map<Long, Long> waitU;      // XID正在等待的UID

    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    // 不需要等待则返回null，否则返回锁对象
    // 会造成死锁则抛出异常
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 当前的事务是否持有这个资源
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            // 当前事务不持有这个uid的资源,我们对于u2x和x2u的集合进行更新
            if(!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            // 如果持有的话,那当前的事务就是在等待中,我们更新等待队列的集合
            waitU.put(xid, uid);
            //putIntoList(wait, xid, uid);
            putIntoList(wait, uid, xid);
            // 出现了死锁.
            // 事务和连接的边都应该撤销.
            if(hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw Error.DeadlockException;
            }
            // 上锁并且放进去.
            Lock l = new ReentrantLock();
            // 这就是阻塞了,被lock.
            l.lock();
            // xid持有的lock
            waitLock.put(xid, l);
            return l;

        } finally {
            lock.unlock();
        }
    }

    // 在一个事务 commit 或者 abort 时，就可以释放所有它持有的锁，并将自身从等待图中删除。
    public void remove(long xid) {
        lock.lock();
        // 不存在,本次访问有效,打上一个stamp
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    // 把自己持有的资源全部释放
                    Long uid = l.remove(0);
                    // (唤醒) 从阻塞队列中选取一个xid处理这个uid资源
                    selectNewXID(uid);
                }
            }
            // 删除等待关系.
            waitU.remove(xid);
            // 删除xid的等待列表.
            x2u.remove(xid);
            // 删除xid持有的lock.
            waitLock.remove(xid);

        } finally {
            lock.unlock();
        }
    }

    // 从等待队列中选择一个xid来占用uid
    private void selectNewXID(long uid) {
        // 获得自由,不再被某个xid所持有
        u2x.remove(uid);
        // 等待这个uid的xid列表.
        List<Long> l = wait.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        // 有thread在进行等待.
        while(l.size() > 0) {
            long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            } else {
                // 从等待队列拿出来的xid开始持有这个lock.
                u2x.put(uid, xid);
                // 从描述lock的集合中移除.
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if(l.size() == 0) wait.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    // 这里做dfs检测是否有回环
    // 图不一定是连通图,但是只要出现circle,一定是死锁.
    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for(long xid : x2u.keySet()) {
            // 拿到一个xid开始dfs
            Integer s = xidStamp.get(xid);
            // 如果被打过时间戳,这意味着是被访问过的,直接跳过
            if(s != null && s > 0) {
                continue;
            }
            // 每一轮访问都会 + 1
            stamp ++;
            if(dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        // 这意味着在本轮的访问中,我们找到了一个circle,出现了死锁
        if(stp != null && stp == stamp) {
            return true;
        }
        // 上一轮的,直接跳过.
        if(stp != null && stp < stamp) {
            return false;
        }
        // 不存在,本次访问有效,打上一个stamp
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                i.remove();
                break;
            }
        }
        if(l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if(!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    // 返回uid0持有的列表中是否有uid1
    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if(l == null) return false;
        Iterator<Long> i = l.iterator();
        while(i.hasNext()) {
            long e = i.next();
            if(e == uid1) {
                return true;
            }
        }
        return false;
    }

}
