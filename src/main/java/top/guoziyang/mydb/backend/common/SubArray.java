package top.guoziyang.mydb.backend.common;

// SubArray表示一个字节数组的子数组视图,这是为了解决Java中没有指针的缺陷
// 它包含一个原始字节数组raw,以及子数组的起始位置start和结束位置end
// 通过SubArray,我们可以方便地操作字节数组的某个部分,而不需要复制数据
// 例如,我们可以创建一个SubArray来表示一个记录在页面中的位置,然后通过start和end来访问该记录的数据
// 这样可以提高性能,减少内存使用
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
