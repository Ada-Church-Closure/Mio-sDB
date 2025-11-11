package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度,也就是所有要写入的字节都会放在Data中,相当于就是一种备份
 * Checksum 4字节int
 *
 * log文件本身在磁盘而不是内存中,所以每次读写都要进行IO操作
 * 你要知道:日志是数据库系统中非常重要的一部分,它记录了所有对数据库的修改操作
 * 在数据库崩溃后,可以通过日志进行恢复操作,确保数据的一致性和完整性,所以必须放在磁盘上
 */
public class LoggerImpl implements Logger {
    // 计算Checksum的种子
    private static final int SEED = 13331;
    // 日志各字段的偏移量
    // OF_SIZE: Size字段的偏移量
    // OF_CHECKSUM: Checksum字段的偏移量
    // OF_DATA: Data字段的偏移量
    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    // 检查并移除bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        // 遍历所有日志，计算checksum
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        // 对比checksum，不匹配和bad tail无关,因为有bad tail只是说明最后一条日志不完整
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            // 截断文件，移除bad tail
            // 这里的思想:没有成功写入log的话,我们就当它没发生过,直接把文件截断到最后一条完整日志的位置
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            // 将文件指针移回position位置
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    // 计算一条日志的checksum
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    /**
     * 写入一条日志,我们把data包装成标准日志格式再写入文件
     * @param data 要写入的日志数据
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        // 每次写入一条新的日志后,都要更新xChecksum
        updateXChecksum(log);
    }

    // 更新XChecksum
    private void updateXChecksum(byte[] log) {
        // 每次都会加入新日志,所以要基于旧的xChecksum计算新的xChecksum
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum)));
            // 计算出来的xChecksum写入文件头后,强制写入磁盘
            fc.force(false);
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    // 将data包装成标准日志格式
    private byte[] wrapLog(byte[] data) {
        // 计算Checksum和Size
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        // 拼接字节数组并返回
        return Bytes.concat(size, checksum, data);
    }

    // 截断日志文件
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    // 采用类似迭代器的方式读取下一条日志
    // 返回包含Size, Checksum, Data的完整日志
    private byte[] internNext() {
        // 检查是否还能读取Size
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        // 读取Size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            // 把size的大小读取到tmp中
            fc.position(position);
            fc.read(tmp);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 转成int
        int size = Parser.parseInt(tmp.array());
        if(position + size + OF_DATA > fileSize) {
            return null;
        }
        // 读取完整日志中的Data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch(IOException e) {
            Panic.panic(e);
        }
        // 验证Checksum, 不通过则返回null
        byte[] log = buf.array();
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {
            return null;
        }
        // 移动position指针,返回日志,现在position指向下一条日志的开头
        position += log.length;
        return log;
    }

    // 这里是调用internNext并只返回Data部分
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    // 将position指针重置到日志开头，准备重新读取日志
    // rewind本身的语义是指磁带倒带,这里借用这个词,表示将日志读取指针重置到开头
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
