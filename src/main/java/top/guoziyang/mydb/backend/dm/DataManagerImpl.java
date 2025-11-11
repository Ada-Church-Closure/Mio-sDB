package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.dm.pageIndex.PageIndex;
import top.guoziyang.mydb.backend.dm.pageIndex.PageInfo;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    // 我们insert一个数据,比较复杂
    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // 把送进来的数据转成字节数组
        byte[] raw = DataItem.wrapDataItemRaw(data);
        // 数据大于了一个page,不允许!
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 尝试获取一个有足够空间的page
        PageInfo pi = null;
        // 尝试5次获取page的机会,不会无限循环等待
        for(int i = 0; i < 5; i ++) {
            // 利用索引结构获取一个有足够空间的page
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                // 如果没有合适的page,就新建一个page
                int newPgno = pc.newPage(PageX.initRaw());
                // 将新page加入索引结构
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        // 上面是在找能插入数据的page号,下面是真正插入数据.
        // 从pageCache中取出这个page,然后插入数据.
        Page pg = null;
        int freeSpace = 0;
        try {
            // 取出page
            pg = pc.getPage(pi.pgno);
            // 先写日志 WAL原则
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);

            // 记录偏移量
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 将取出的pg重新插入pIndex
            // 之前select的时候已经把这个page从index中删掉了
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                // 取page失败,但是已经从index中删掉了,所以要把原来的freeSpace加回去
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();
        // close的时候,要更新一下vc,防止下次打开时校验失败
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 为xid生成update日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    // DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。DataItem 存储的 key(就是 long类型的uid)，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgno);
        // data item的数据结构决定了这是可以解析的,返回一个data item的对象.
        return DataItem.parseDataItem(pg, offset, this);
    }

    // 读写是按照page进行的,直接释放一整个page.
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化pageIndex
    // 创建的时候,就会给每个存在的页面创建索引index.
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            // 创建完index之后,及时退出,防止缓存溢出
            pg.release();
        }
    }
    
}
