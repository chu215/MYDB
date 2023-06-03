package top.chu.mydb.backend.tm;

import top.chu.mydb.backend.utils.Panic;
import top.chu.mydb.backend.utils.Parser;
import top.chu.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TransactionManagerImpl implements TransactionManager {

    // XID文件头长度，记录XID文件管理的事务个数
    static final int LEN_XID_HEADER_LENGTH = 8;

    // 每个事务占用长度
    public static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    public static final byte FIELD_TRAN_ACTIVE = 0;
    public static final byte FIELD_TRAN_COMMITED = 1;
    public static final byte FIELD_TRAN_ABORTED = 2;

    // 超级事务，永远为commited状态
    public static final long SUPER_XID = 0;

    // XID文件后缀
    static final String XID_SUFFIX = ".xid";

    private RandomAccessFile file;

    private FileChannel fc;

    private long xidCounter;

    private Lock counterLock;

    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }
        if (fileLen < LEN_XID_HEADER_LENGTH) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter = Parser.parseLong(buf.array());
        long end = getXidPosition(this.xidCounter + 1);
        if (end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务id取得其在文件中的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    // 根据xid更新文件中的事务状态
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);

        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(false); // 强制将文件缓冲区的内容写入磁盘，false表示只写入文件内容，不写入元数据
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将xidCounter加一，并更新xid header的内容
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
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

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    @Override
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITED);
    }

    @Override
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.allocate(XID_FIELD_SIZE);

        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    @Override
    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITED);
    }

    @Override
    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
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
