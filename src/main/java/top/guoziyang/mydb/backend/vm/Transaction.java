package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

// vm对一个事务的抽象,目的是解决不可重复读的问题
// 保存了一个快照,用来判断某个事务是否在另一个事务开始时是活跃的
public class Transaction {
    // 事务id
    // 事务隔离级别
    // 事务开始时的活跃事务快照
    // 事务错误信息
    // 事务是否被自动中止
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    /**
     * 创建一个新的事务对象,并根据隔离级别生成快照,snapshot中包含了所有在该事务开始时活跃的事务id
     * @param xid 事务id
     * @param level 事务隔离级别
     * @param active 当前活跃的事务列表
     * @return 新的事务对象
     */
    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    // 判断某个事务是否存在于该事务的快照中
    public boolean isInSnapshot(long xid) {
        // 超级事务永远不可见
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
