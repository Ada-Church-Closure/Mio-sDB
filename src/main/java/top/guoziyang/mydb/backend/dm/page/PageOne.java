package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.RandomUtil;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭,如果没有正常关闭，则说明可能有未刷新的数据，需要进行恢复
 * 恢复就是利用日志将未刷新的数据重新写入页面
 */
// TODO 打开关闭的次数过多难道不会写穿这个pageOne?是否使用循环写入更好?
public class PageOne {
    // VC: Valid Check,100是字节偏移,8字节长度,偏移100是为了避开常用的元数据区域
    private static final int OF_VC = 100;
    // VC长度8字节
    private static final int LEN_VC = 8;

    // 初始化第一页,设置为打开状态
    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    // 设置VC为打开状态,写入随机字节
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }

    // 设置VC为打开状态,写入随机字节
    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    // 设置VC为关闭状态,将前8字节拷贝到后8字节
    public static void setVcClose(Page pg) {
        pg.setDirty(true);
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    // 检查VC是否是合法的关闭状态
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    // 检查VC是否是合法的关闭状态
    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC), Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC));
    }
}
