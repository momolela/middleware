package com.momolela;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class ChangeConfFileName {
    public void renameFile(String file, String toFile) {
        File toBeRenamed = new File(file);
        //检查要重命名的文件是否存在，是否是文件
        if (!toBeRenamed.exists() || toBeRenamed.isDirectory()) {
            System.out.println("文件不存在: " + file);
            return;
        }
        File newFile = new File(toFile);
        //修改文件名
        if (toBeRenamed.renameTo(newFile)) {
            System.out.println("重命名成功.");
        } else {
            System.out.println("重命名失败");
        }
    }

    public static boolean judgeOs() {
        String os = System.getProperty("os.name").toLowerCase();
        if (os != null && os.startsWith("windows")) {
            return true;
        } else {
            return false;
        }
    }

    public boolean isLinux() {
        File file = new File("D:\\temp\\nginx-linux.config");
        if (!file.exists()) {
            System.out.println("文件nginx-linux.config不存在");
            return true;
        } else {
            System.out.println("文件nginx-linux.config存在");
            return false;
        }
    }

    private static void startWinProc() throws IOException {
        //String myExe = "cmd /c start nginx";
        String myExe = "cmd /c nginx -s reload";
        String CONFPREFIXURL = System.getProperty("user.dir") + File.separator + "nginx" + File.separator + "windows";
        System.out.println(CONFPREFIXURL);
        String path = "D:\\nginx";
        File dir = new File(path);
        String[] str = new String[]{};
        // 执行命令
        Runtime.getRuntime().exec(myExe, str, dir);
    }

    private static void startLinuxProc() throws IOException {
        System.out.println("开启进程:" + "nginx");
        String command1 = "/usr/local/nginx/sbin/nginx";

        String pro = executeCmd2(command1);
        System.out.println(pro);
    }

    /**
     * @desc 执行cmd命令
     */
    public static String executeCmd(String command) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec("cmd /c " + command);
        // Process process = runtime.exec( command);
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
        String line = null;
        StringBuilder build = new StringBuilder();
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            build.append(line);
        }
        return build.toString();
    }

    /**
     * @desc 执行cmd命令
     */
    public static String executeCmd2(String command) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        Process process = runtime.exec(command);
        BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream(), "UTF-8"));
        String line = null;
        StringBuilder build = new StringBuilder();
        while ((line = br.readLine()) != null) {
            System.out.println(line);
            build.append(line);
        }
        return build.toString();
    }

    public static void main(String[] args) {
        ChangeConfFileName cfn = new ChangeConfFileName();
        if (cfn.isLinux()) {
            cfn.renameFile("D:\\temp\\nginx.config", "D:\\temp\\nginx-linux.config");
            cfn.renameFile("D:\\temp\\nginx-windows.config", "D:\\temp\\nginx.config");
        } else {
            cfn.renameFile("D:\\temp\\nginx.config", "D:\\temp\\nginx-windows.config");
            cfn.renameFile("D:\\temp\\nginx-linux.config", "D:\\temp\\nginx.config");
        }
        try {
            startWinProc();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
