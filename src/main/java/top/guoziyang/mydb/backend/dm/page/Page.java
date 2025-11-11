package top.guoziyang.mydb.backend.dm.page;

// 页面接口,定义了页面的基本操作
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
