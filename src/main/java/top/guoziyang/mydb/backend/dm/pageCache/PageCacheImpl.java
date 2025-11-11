package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

// 页面缓存实现类,实现了页面缓存接口,继承自抽象缓存类
// 这里相当于磁盘就是一个文件系统,页面就是文件系统中的块
// 页面缓存负责将文件系统中的页面读入内存,并进行缓存管理
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    // 页面大小为8KB,这是数据库系统中常用的页面大小,MEM_MIN_LIM表示内存下限,至少要10页
    private static final int MEM_MIN_LIM = 10;
    // 数据库文件后缀名
    public static final String DB_SUFFIX = ".db";

    // 数据库文件相关变量
    // RandomAccessFile用于文件读写,RandomAccessFile支持随机访问文件，可以在文件的任意位置读写数据
    private RandomAccessFile file;
    // FileChannel用于文件读写,FileChannel提供了更高效的文件读写操作，支持异步IO和内存映射等高级功能
    // 这里的fc用于读写数据库文件,与file配合使用
    private FileChannel fc;
    // 保护文件读写的锁
    private Lock fileLock;
    // 当前页面数量的原子变量
    private AtomicInteger pageNumbers;

    // 构造函数,传入已经打开的数据库文件的RandomAccessFile和FileChannel
    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    // 创建新页面,返回页面编号
    // initData用于初始化页面数据
    public int newPage(byte[] initData) {
        // 分配新的页面编号
        // 注意:一条记录只能占用一个页面,不能跨页面存储,这意味着一条记录的大小不能超过页面大小
        int pgno = pageNumbers.incrementAndGet();
        // 创建新的页面对象
        Page pg = new PageImpl(pgno, initData, null);
        // 一个新建的页面一定要立刻写回到磁盘中
        flush(pg);
        return pgno;
    }
    // 获取页面,根据页面编号返回页面对象
    public Page getPage(int pgno) throws Exception {
        return get((long)pgno);
    }

    /**
     * 根据pageNumber从数据库文件中读取页数据，并包裹成Page,供缓存使用
     * @param key 页面编号
     * @return 页面对象
     * @throws Exception 读取文件异常时抛出
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        // 计算页面在文件中的偏移位置
        int pgno = (int)key;
        // 页面在文件中的偏移位置
        long offset = PageCacheImpl.pageOffset(pgno);
        // 从文件中读取页面数据
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();
        try {
            // 将文件通道的位置移动到页面的偏移位置
            fc.position(offset);
            // 读取页面数据到缓冲区
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();
        // 包裹成Page对象并返回
        return new PageImpl(pgno, buf.array(), this);
    }

    /**
     * 将页面写回到数据库文件
     * @param pg 页面对象
     */
    @Override
    protected void releaseForCache(Page pg) {
        // 只有脏页面才需要写回
        if(pg.isDirty()) {
            flush(pg);
            // 页面写回后,将脏标记清除
            pg.setDirty(false);
        }
    }

    // 释放页面,将页面从缓存中移除
    public void release(Page page) {
        release((long)page.getPageNumber());
    }

    // 将页面内容刷新到磁盘
    public void flushPage(Page pg) {
        flush(pg);
    }
    /**
     * 将页面写回到数据库文件
     * @param pg 页面对象
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();
        long offset = pageOffset(pgno);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 获取当前页面数量
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    private static long pageOffset(int pgno) {
        // 页面编号从1开始,所以要减1
        return (pgno-1) * PAGE_SIZE;
    }
    
}
