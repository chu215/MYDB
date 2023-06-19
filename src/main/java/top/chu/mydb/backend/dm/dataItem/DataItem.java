package top.chu.mydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.chu.mydb.backend.common.SubArray;
import top.chu.mydb.backend.dm.DataManagerImpl;
import top.chu.mydb.backend.dm.page.Page;
import top.chu.mydb.backend.utils.Parser;
import top.chu.mydb.backend.utils.Types;

import java.util.Arrays;

public interface DataItem {
    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, DataItemImpl.OF_SIZE, DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }
}
