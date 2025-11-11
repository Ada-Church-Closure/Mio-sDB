package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

// 页面缓存接口,定义了页面缓存的基本操作
public interface PageCache {
    // 页面大小为8KB,这是数据库系统中常用的页面大小
    public static final int PAGE_SIZE = 1 << 13;

    // 创建新页面,返回页面编号
    int newPage(byte[] initData);
    // 获取页面,根据页面编号返回页面对象
    Page getPage(int pgno) throws Exception;
    // 关闭页面缓存,释放相关资源
    void close();
    // 释放页面,将页面从缓存中移除
    void release(Page page);

    // 将页面写回到磁盘,Bgno表示截止到哪个页面编号的页面都要写回
    void truncateByBgno(int maxPgno);
    // 获取当前页面数量
    int getPageNumber();
    // 将页面内容刷新到磁盘,和truncateByBgno的区别是只刷新单个页面
    void flushPage(Page pg);

    // 工厂方法,创建新的页面缓存
    public static PageCacheImpl create(String path, long memory) {
        // 创建数据库文件,一个database对应一个文件
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        // 创建页面缓存对象,传入文件相关变量和内存大小
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }

    public static PageCacheImpl open(String path, long memory) {
        File f = new File(path+PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory/PAGE_SIZE);
    }
}
