package com.orm.hbase.scanner;

import com.google.common.collect.Lists;
import com.orm.hbase.annotations.Table;
import com.orm.hbase.utils.HBaseUtils;

import java.io.File;
import java.util.List;

/**
 * @author zap
 * @version 1.0, 2017/12/25
 */
public class HBaseTableScanner {

    private String basePackage;
    private boolean scanSubPackage;

    public HBaseTableScanner(String basePackage, boolean scanSubPackage) {
        setBasePackage(basePackage);
        setScanSubPackage(scanSubPackage);
    }

    /**
     * 扫描包
     */
    public void execute() throws Exception {
        String basePath = basePackage.replace(".", File.separator);

        @SuppressWarnings("all")
        String path = Thread.currentThread().getContextClassLoader().getResource("").getPath();

        String classPath = path.replace("/", "\\").replaceFirst("\\\\", "");
        File file = new File(path + basePath);

        List<String> classNameList = getClassList(file, classPath, scanSubPackage);

        // 加载类
        List<Class<?>> classList = loadClass(classNameList);
        for (Class<?> clazz : classList) {
            HBaseTableCache.instance().put(clazz, HBaseUtils.findColumnSchemaList(clazz));
        }

    }

    private List<Class<?>> loadClass(List<String> classNameList) throws Exception {
        List<Class<?>> classList = Lists.newArrayList();

        for (String className : classNameList) {
            Class<?> clazz = Class.forName(className);
            if (clazz.isAnnotationPresent(Table.class)) {
                classList.add(clazz);
            }
        }

        return classList;
    }

    private List<String> getClassList(File file, String classPath, final boolean scanSubPackage) {
        List<String> classNameList = Lists.newArrayList();

        if (file.isDirectory()) {
            File[] files = file.listFiles((dir, name) ->
                    scanSubPackage ? dir.isDirectory() || name.endsWith(".class") : name.endsWith(".class"));

            if (null != files) {
                for (File f : files) {
                    classNameList.addAll(getClassList(f, classPath, scanSubPackage));
                }
            }
        } else {
            String className = file.getPath().replace(classPath, "")
                    .replace("\\", ".").replace(".class", "");
            classNameList.add(className);
        }

        return classNameList;
    }

    private void setScanSubPackage(boolean scanSubPackage) {
        this.scanSubPackage = scanSubPackage;
    }


    private void setBasePackage(String basePackage) {
        if (null == basePackage) {
            throw new IllegalArgumentException("扫描基本包不能为空");
        }
        if (basePackage.trim().isEmpty()) {
            throw new IllegalArgumentException("扫描基本包不能为空");
        }
        this.basePackage = basePackage;
    }

}
