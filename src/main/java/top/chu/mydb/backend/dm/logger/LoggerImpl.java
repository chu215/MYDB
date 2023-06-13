package top.chu.mydb.backend.dm.logger;

import com.google.common.primitives.Bytes;
import top.chu.mydb.backend.utils.Panic;
import top.chu.mydb.backend.utils.Parser;
import top.chu.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 日志文件标准格式：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum为后续所有日志计算的校验和
 * 每条日志正确的格式：
 * [Size] [Checksum] [Data]
 * Size 4字节int 表示Data的字节数
 * Checksum 4字节int 该条日志的校验和
 */
public class LoggerImpl implements Logger {
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;

    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;
    public static final String LOG_SUFFIX = ".log";
    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;
    private long position; // 当前日志指针的位置
    private long fileSize;
    private int xCheckSum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }
    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.file = raf;
        this.fc = fc;
        this.xCheckSum = xCheckSum;
        lock = new ReentrantLock();
    }

    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if (size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xCheckSum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xCheckSum = xCheckSum;

        checkAndRemoveTail();
    }

    // 检查并移除 bad tail
    private void checkAndRemoveTail() {
        rewind();

        int xCheck = 0;
        while (true) {
            byte[] log = internNext();
            if (log == null) break;
            xCheck = calChecksum(xCheck, log);
        }
        if (xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException);
        }

        // 截断文件到正常日志的末尾
        try {
            truncate(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();
    }

    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    private byte[] internNext() {
        if (position + OF_DATA >= fileSize) {
            return null;
        }
        // 读取size
        ByteBuffer tmp = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(tmp);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array());
        if (position + OF_DATA + size > fileSize) {
            return null;
        }
        // 读取checksum + data
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();

        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if (checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        ByteBuffer buf = ByteBuffer.wrap(log);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);
    }

    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data));
        byte[] size = Parser.int2Byte(data.length);
        return Bytes.concat(size, checksum, data);
    }

    private void updateXChecksum(byte[] log) {
        this.xCheckSum = calChecksum(this.xCheckSum, log);
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(this.xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if (log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
