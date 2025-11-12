package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

public class Visibility {

    // 是否有版本跳跃的问题
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            // 读提交不存在问题
            return false;
        } else {
            // 之前有一个事务:
            //      修改了并且已经提交 && 这个事务还是我不可见的事务!
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 读提交,判断一个版本对事务t是否可见
     * @param tm 事务管理器
     * @param t 事务
     * @param e 记录
     * @return 版本是否对事务可见
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 自己创建的记录且未删除
        if(xmin == xid && xmax == 0) return true;

        // 创建该记录的事务已提交
        if(tm.isCommitted(xmin)) {
            // 并且还未被删除,就是可见的
            if(xmax == 0) return true;
            // 否则,如果删除该记录的事务不是自己且未提交,也是可见的
            // 因为如果提交了,说明记录被删除了,对当前事务不可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 可重复读,判断一个版本对事务t是否可见
     * @param tm 事务管理器
     * @param t 事务
     * @param e 记录
     * @return 版本是否对事务可见,在可重复读隔离级别下
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();
        // 自己创建的记录且未删除
        if(xmin == xid && xmax == 0) return true;

        // 创建该记录的事务已提交,且在当前事务开始前就已经提交,且不在当前事务的活跃快照中
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 并且还未被删除,就是可见的
            if(xmax == 0) return true;
            // 否则,如果删除该记录的事务不是自己且未提交,或者在当前事务的活跃快照中,也是可见的
            // 说白了,在我可重复读级别看来,你就是没删除也没修改
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
