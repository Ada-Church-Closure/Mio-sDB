package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;

// 向上层提供功能
// 同时，VM 的实现类还被设计为 Entry 的缓存，需要继承 AbstractCache<Entry>。需要实现的获取到缓存和从缓存释放的方法很简单：
public interface VersionManager {
    // 这里的接口就比较直观了.数据操作
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    // 事务的操作.
    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
