package top.guoziyang.mydb.backend.tm;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

public interface TransactionManager {
    // 开启事务
    long begin();
    // 提交事务
    void commit(long xid);
    // 取消事务
    void abort(long xid);
    // 查询事务状态
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    // 关闭事务管理器
    void close();

    // 这两个静态方法用于创建或打开TransactionManager实例
    // path是不包含后缀名的文件路径

    // 创建一个新的TransactionManager实例
    public static TransactionManagerImpl create(String path) {
        // 创建XID文件
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
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

        // 写空XID文件头
        // 初始化xidCounter为0,占8字节,表示当前没有事务
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        
        return new TransactionManagerImpl(raf, fc);
    }

    // 打开一个已有的TransactionManager实例,path是不包含后缀名的文件路径
    // 例如传入"/data/mydb"会打开"/data/mydb.xid"文件
    public static TransactionManagerImpl open(String path) {
        // 打开XID文件
        File f = new File(path+TransactionManagerImpl.XID_SUFFIX);
        // 检查文件是否存在且可读写
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }
        // 打开文件
        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
           Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }
}
