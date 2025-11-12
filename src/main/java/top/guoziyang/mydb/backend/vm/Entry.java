package top.guoziyang.mydb.backend.vm;

import java.util.Arrays;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * VM向上层抽象出entry,这是实现MVCC的基础
 * entry结构：
 * XMIN: 8字节,表示创建该entry的事务ID
 * XMAX: 8字节,表示删除该entry的事务ID,如果未删除则为0
 * data: n字节,表示实际存储的数据
 * [XMIN] [XMAX] [data]
 * 记录 XMIN 和 XMAX 用于实现多版本并发控制(MVCC)
 * 通过检查事务ID,可以确定某个事务是否可以看到该entry
 * 例如,如果一个事务的ID大于XMIN且小于XMAX,则该事务可以看到该entry(这意味着在删除之前并且在创建之后)
 * (这就是entry的核心设计思想)
 * 如果一个事务的ID小于XMIN,则该entry对该事务不可见
 * 如果一个事务的ID大于XMAX,则该entry对该事务不可见
 * MVCC允许多个事务同时操作数据库而不会相互干扰,提高了并发性能
 * 通过entry,上层模块可以方便地实现事务隔离和一致性
 */
public class Entry {

    // 各字段的偏移量
    // XMIN偏移量 0
    // XMAX偏移量 8
    // data偏移量 16
    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN+8;
    private static final int OF_DATA = OF_XMAX+8;

    private long uid;
    // 一条记录存储于一个dataItem中
    private DataItem dataItem;
    private VersionManager vm;

    /**
     * 创建一个新的entry
     * @param vm 版本管理器
     * @param dataItem 数据项
     * @param uid 数据项的唯一标识符
     * @return 新的entry对象
     */
    public static Entry newEntry(VersionManager vm, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.vm = vm;
        return entry;
    }

    /**
     * 从版本管理器中加载entry
     * @param vm 版本管理器
     * @param uid 数据项的唯一标识符
     * @return 加载的entry对象
     * @throws Exception 加载过程中可能抛出的异常
     */
    public static Entry loadEntry(VersionManager vm, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
        return newEntry(vm, di, uid);
    }

    /**
     * 包装entry的原始字节数据
     * @param xid 事务ID
     * @param data 实际数据字节数组
     * @return 包装后的字节数组
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        // 构造entry的字节数组
        // 前8字节为XMIN,后8字节为XMAX(初始为0),后面跟实际数据
        byte[] xmin = Parser.long2Byte(xid);
        // XMAX初始为0
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl)vm).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    // 以拷贝的形式返回内容
    // 相当于反序列化data部分
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start+OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        // 修改XMAX需要在事务内进行
        // 调用before和after方法,因为这是对dataItem的修改
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    // 获取数据项的唯一标识符
    public long getUid() {
        return uid;
    }
}
