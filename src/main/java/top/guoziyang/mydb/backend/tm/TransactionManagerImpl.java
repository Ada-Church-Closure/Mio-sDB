package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

// 我们称XID是一个全局的文件,xid是一个事务持有的标识符
// XID文件格式:
// XID_FILE_HEADER:
// | 8 bytes |  --> xidCounter 事务的总数量,也就是最大事务的编号.
// XID_FILE_BODY:
// | 1 byte  |  --> xid状态(0:active, 1:committed, 2:aborted)
// | 1 byte  |  --> xid状态
// | 1 byte  |  --> xid状态
// ...
public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度: 8字节，存储xidCounter.记录当前管理的事务的个数
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 每个事务的占用长度
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;
	private static final byte FIELD_TRAN_COMMITTED = 1;
	private static final byte FIELD_TRAN_ABORTED  = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    // XID文件后缀名
    static final String XID_SUFFIX = ".xid";

    // XID文件相关变量
    // RandomAccessFile用于文件读写
    private RandomAccessFile file;
    // FileChannel用于文件读写
    private FileChannel fc;
    // 当前XID计数器
    private long xidCounter;
    // 保护XID计数器的锁
    // 这种lock的工作原理类似于synchronized，但更灵活
    // synchronized在同一时刻只能有一个线程访问临界区，而lock可以实现更复杂的同步
    // 例如，可以尝试获取锁，如果获取不到就执行其他操作，而不是一直等待
    // 这里使用lock是为了在begin()方法中保护xidCounter的自增和XID状态更新操作的原子性
    // 确保在多线程环境下，多个线程不会同时修改xidCounter，避免数据竞争和不一致
    private Lock counterLock;

    // 构造函数,传入已经打开的XID文件的RandomAccessFile和FileChannel
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            // 文件长度获取失败
            Panic.panic(Error.BadXIDFileException);
        }
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            // 文件长度小于XID文件头长度，非法
            Panic.panic(Error.BadXIDFileException);
        }
        // 读取XID文件头中的xidCounter
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            // 读取文件头,position设为0,position读完后会自动后移
            fc.position(0);
            // 读取文件头内容到buf
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        // 解析xidCounter
        this.xidCounter = Parser.parseLong(buf.array());
        // 计算理论文件长度
        long end = getXidPosition(this.xidCounter + 1);
        // 对比实际文件长度和理论文件长度
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在XID文件中对应的位置
    private long getXidPosition(long xid) {
        // 位置 = 文件头长度(8) + (xid - 1) * 每个事务占用长度(1)
        return LEN_XID_HEADER_LENGTH + (xid-1) * XID_FIELD_SIZE;
    }

    // 更新xid事务的状态为status
    private void updateXID(long xid, byte status) {
        // 计算xid在文件中的位置
        long offset = getXidPosition(xid);
        // 准备写入的字节数组
        byte[] tmp = new byte[XID_FIELD_SIZE];
        // 设置状态
        tmp[0] = status;
        // 将状态写入文件
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            // 定位到xid位置
            fc.position(offset);
            // 写入状态
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter ++;
        // 更新XID文件头中的xidCounter
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            // 定位到文件头位置
            fc.position(0);
            // 写入新的xidCounter
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // 强制将更改写入磁盘
            // 这里传入false表示不需要同时将文件元数据也写入磁盘
            // 因为我们只修改了文件内容，不涉及文件大小等元数据的变化
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 开始一个事务，并返回XID
    // begin主要是要进行两件事:
    // 1. 分配一个新的xid
    // 2. 将该xid的状态设置为active
    public long begin() {
        counterLock.lock();
        try {
            // 分配一个新的xid,这是自增并且全局的变量
            long xid = xidCounter + 1;
            // 将该xid的状态设置为active
            updateXID(xid, FIELD_TRAN_ACTIVE);
            // xidCounter加一
            incrXIDCounter();
            // 返回该xid
            return xid;
        } finally {
            // 释放锁
            counterLock.unlock();
        }
    }

    // 提交XID事务
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于某个status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
