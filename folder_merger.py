import os
import shutil
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import threading
import time
from pathlib import Path
import queue
import logging
import json
import re
import sys

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_application_path():
    """获取应用程序所在路径（兼容打包后的 EXE 和脚本运行）"""
    if getattr(sys, 'frozen', False):
        # 如果是打包后的 EXE，sys.executable 是 EXE 的全路径
        return os.path.dirname(sys.executable)
    else:
        # 如果是脚本，__file__ 是脚本的路径
        return os.path.dirname(os.path.abspath(__file__))

CONFIG_FILE = os.path.join(get_application_path(), 'merger_config.json')

class MergerCore:
    def __init__(self, update_callback=None, log_callback=None):
        self.update_callback = update_callback
        self.log_callback = log_callback
        self.stop_flag = False

    def log(self, message):
        if self.log_callback:
            self.log_callback(message)
        logging.info(message)

    def scan_files(self, source_dir):
        """生成器：扫描所有文件"""
        count = 0
        for root, dirs, files in os.walk(source_dir):
            if self.stop_flag:
                break
            for file in files:
                if self.stop_flag:
                    break
                yield os.path.join(root, file)
                count += 1
                if count % 1000 == 0:
                    if self.update_callback:
                        self.update_callback("scanning", count, None)

    def get_unique_filename(self, target_dir, filename):
        """处理文件名冲突，返回唯一文件名"""
        base_name, ext = os.path.splitext(filename)
        counter = 1
        new_filename = filename
        while os.path.exists(os.path.join(target_dir, new_filename)):
            new_filename = f"{base_name} ({counter}){ext}"
            counter += 1
        return new_filename

    def get_writable_targets(self, target_parent, limit):
        """生成器：返回可写入的目标文件夹路径和剩余容量"""
        existing_folders = {}
        max_index = 0
        
        try:
            if not os.path.exists(target_parent):
                os.makedirs(target_parent, exist_ok=True)
            
            # 1. 扫描现有文件夹，建立索引映射
            for name in os.listdir(target_parent):
                if os.path.isdir(os.path.join(target_parent, name)):
                    match = re.match(r'^Merged_(\d+)$', name)
                    if match:
                        index = int(match.group(1))
                        existing_folders[index] = name
                        if index > max_index:
                            max_index = index
            
            # 2. 遍历从 1 到 max_index 的所有可能索引（填充空缺和未满的）
            if max_index > 0:
                for i in range(1, max_index + 1):
                    if i in existing_folders:
                        dir_name = existing_folders[i]
                        dir_path = os.path.join(target_parent, dir_name)
                        # 计算现有文件数
                        current_count = len([name for name in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, name))])
                    else:
                        # 文件夹缺失，需要创建
                        dir_name = f"Merged_{i}"
                        dir_path = os.path.join(target_parent, dir_name)
                        current_count = 0
                        os.makedirs(dir_path, exist_ok=True)
                    
                    remaining = limit - current_count
                    if remaining > 0:
                        yield dir_path, remaining
            
            # 3. 如果所有现有文件夹都满了，或没有现有文件夹，从 max_index + 1 开始创建新文件夹
            next_index = max_index + 1
            while True:
                dir_name = f"Merged_{next_index}"
                dir_path = os.path.join(target_parent, dir_name)
                os.makedirs(dir_path, exist_ok=True)
                yield dir_path, limit
                next_index += 1
                
        except Exception as e:
            self.log(f"扫描目标文件夹时出错: {e}")
            # 出错后的兜底策略
            i = 1
            while True:
                dir_path = os.path.join(target_parent, f"Merged_{i}")
                os.makedirs(dir_path, exist_ok=True)
                yield dir_path, limit
                i += 1

    def process(self, config):
        source_dir = config['source_dir']
        target_parent = config['target_parent']
        limit = config['files_per_folder']
        op_mode = config['operation_mode']
        rename_mode = config['rename_mode']
        conflict_mode = config['conflict_mode']
        prefix = config['custom_prefix']

        self.log(f"开始处理: 源={source_dir}, 目标父目录={target_parent}")
        
        # 1. 扫描文件
        self.log("正在扫描文件...")
        all_files = []
        for file_path in self.scan_files(source_dir):
            if self.stop_flag:
                self.log("操作已取消")
                return
            all_files.append(file_path)
        
        total_files = len(all_files)
        self.log(f"扫描完成，共找到 {total_files} 个文件")

        # 2. 处理文件
        processed_count = 0
        
        # 获取目标文件夹生成器
        target_gen = self.get_writable_targets(target_parent, limit)
        current_target_dir, slots_left = next(target_gen)
        
        # 如果是首次创建，可能需要记录日志
        self.log(f"当前写入目标: {os.path.basename(current_target_dir)} (剩余容量: {slots_left})")

        for src_path in all_files:
            if self.stop_flag:
                break

            # 检查是否需要切换文件夹
            if slots_left <= 0:
                current_target_dir, slots_left = next(target_gen)
                self.log(f"切换到新文件夹: {os.path.basename(current_target_dir)} (容量: {slots_left})")

            # 计算新文件名
            src_path_obj = Path(src_path)
            original_name = src_path_obj.name
            new_name = original_name

            if rename_mode == 'parent_name':
                parent_name = src_path_obj.parent.name
                new_name = f"{parent_name}_{original_name}"
            elif rename_mode == 'prefix':
                # 注意：这里使用 processed_count 可能导致与旧文件重名，
                # 如果是追加模式，最好结合 folder_index 或者使用更复杂的计数。
                # 但为了简单且符合“自定义前缀+序号”，我们继续用全局计数，
                # 但要考虑到如果之前已经有了 file_1.txt，现在再来一个 file_1.txt 会冲突。
                # 冲突处理逻辑会解决这个问题 (get_unique_filename)。
                new_name = f"{prefix}_{processed_count + 1}{src_path_obj.suffix}"
            
            # 处理冲突
            dest_path = os.path.join(current_target_dir, new_name)
            if os.path.exists(dest_path):
                if conflict_mode == 'skip':
                    self.log(f"跳过冲突文件: {new_name}")
                    processed_count += 1 # 视为已处理（虽然是跳过）
                    continue
                elif conflict_mode == 'overwrite':
                    pass # 默认就是覆盖
                elif conflict_mode == 'auto_rename':
                    new_name = self.get_unique_filename(current_target_dir, new_name)
                    dest_path = os.path.join(current_target_dir, new_name)

            # 执行操作
            try:
                if op_mode == 'move':
                    shutil.move(src_path, dest_path)
                    # 及时尝试删除空的父文件夹
                    try:
                        parent_dir = os.path.dirname(src_path)
                        # 确保不删除源根目录
                        if os.path.abspath(parent_dir) != os.path.abspath(source_dir):
                            os.rmdir(parent_dir)
                    except OSError:
                        pass # 文件夹非空，忽略
                else:
                    shutil.copy2(src_path, dest_path)
                
                slots_left -= 1
                processed_count += 1
                
                if processed_count % 100 == 0:
                     if self.update_callback:
                        self.update_callback("processing", processed_count, total_files)

            except Exception as e:
                self.log(f"错误处理文件 {src_path}: {e}")

        # 3. 最终清理空文件夹 (仅在移动模式下，作为兜底)
        if op_mode == 'move' and not self.stop_flag:
            self.log("正在执行最终清理...")
            for root, dirs, files in os.walk(source_dir, topdown=False):
                for name in dirs:
                    try:
                        os.rmdir(os.path.join(root, name))
                    except OSError:
                        pass # 文件夹非空，忽略

        self.log("任务完成!")
        if self.update_callback:
            self.update_callback("done", processed_count, total_files)


class App:
    def __init__(self, root):
        self.root = root
        self.root.title("文件夹合并工具 v1.1.0")
        self.root.geometry("900x700")
        
        self.core = None
        self.thread = None
        
        self._init_vars()
        self._init_ui()

    def _init_vars(self):
        self.source_dir = tk.StringVar()
        self.target_dir = tk.StringVar()
        self.files_per_folder = tk.IntVar(value=10000)
        self.op_mode = tk.StringVar(value="copy")
        self.rename_mode = tk.StringVar(value="keep")
        self.conflict_mode = tk.StringVar(value="auto_rename")
        self.custom_prefix = tk.StringVar(value="File")
        self.progress_var = tk.DoubleVar(value=0)
        self.status_var = tk.StringVar(value="准备就绪")
        
        self.load_config()

    def load_config(self):
        try:
            if os.path.exists(CONFIG_FILE):
                with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    self.source_dir.set(config.get('source_dir', ''))
                    
                    # 路径逻辑
                    saved_target = config.get('target_dir', '')
                    if saved_target:
                         self.target_dir.set(saved_target)
                    elif self.source_dir.get():
                         self.target_dir.set(self.source_dir.get())

                    # 加载其他配置
                    if 'files_per_folder' in config:
                        self.files_per_folder.set(config['files_per_folder'])
                    if 'operation_mode' in config:
                        self.op_mode.set(config['operation_mode'])
                    if 'rename_mode' in config:
                        self.rename_mode.set(config['rename_mode'])
                    if 'conflict_mode' in config:
                        self.conflict_mode.set(config['conflict_mode'])
                    if 'custom_prefix' in config:
                        self.custom_prefix.set(config['custom_prefix'])

        except Exception as e:
            print(f"Error loading config: {e}")

    def save_config(self):
        config = {
            'source_dir': self.source_dir.get(),
            'target_dir': self.target_dir.get(),
            'files_per_folder': self.files_per_folder.get(),
            'operation_mode': self.op_mode.get(),
            'rename_mode': self.rename_mode.get(),
            'conflict_mode': self.conflict_mode.get(),
            'custom_prefix': self.custom_prefix.get()
        }
        try:
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(config, f, ensure_ascii=False, indent=4)
        except Exception as e:
            print(f"Error saving config: {e}")


    def _init_ui(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # 1. 路径选择
        path_frame = ttk.LabelFrame(main_frame, text="路径设置", padding="5")
        path_frame.pack(fill=tk.X, pady=5)

        ttk.Label(path_frame, text="源文件夹 (包含子文件夹):").grid(row=0, column=0, sticky="w")
        ttk.Entry(path_frame, textvariable=self.source_dir, width=50).grid(row=0, column=1, padx=5)
        ttk.Button(path_frame, text="浏览...", command=self.browse_source).grid(row=0, column=2)

        ttk.Label(path_frame, text="目标位置 (合并后的文件夹将在此创建):").grid(row=1, column=0, sticky="w")
        ttk.Entry(path_frame, textvariable=self.target_dir, width=50).grid(row=1, column=1, padx=5)
        ttk.Button(path_frame, text="浏览...", command=self.browse_target).grid(row=1, column=2)

        # 2. 合并设置
        settings_frame = ttk.LabelFrame(main_frame, text="合并设置", padding="5")
        settings_frame.pack(fill=tk.X, pady=5)

        # 分包设置
        ttk.Label(settings_frame, text="每个文件夹文件数量限制:").grid(row=0, column=0, sticky="w")
        ttk.Entry(settings_frame, textvariable=self.files_per_folder, width=10).grid(row=0, column=1, sticky="w")
        
        # 操作模式
        ttk.Label(settings_frame, text="操作模式:").grid(row=1, column=0, sticky="w")
        mode_frame = ttk.Frame(settings_frame)
        mode_frame.grid(row=1, column=1, sticky="w")
        ttk.Radiobutton(mode_frame, text="复制 (保留源文件)", variable=self.op_mode, value="copy").pack(side=tk.LEFT, padx=5)
        ttk.Radiobutton(mode_frame, text="移动 (删除源文件)", variable=self.op_mode, value="move").pack(side=tk.LEFT, padx=5)

        # 3. 文件名处理
        rename_frame = ttk.LabelFrame(main_frame, text="文件名处理", padding="5")
        rename_frame.pack(fill=tk.X, pady=5)

        ttk.Label(rename_frame, text="重命名规则:").grid(row=0, column=0, sticky="w")
        r_frame = ttk.Frame(rename_frame)
        r_frame.grid(row=0, column=1, sticky="w", columnspan=2)
        ttk.Radiobutton(r_frame, text="保留原名", variable=self.rename_mode, value="keep").pack(side=tk.LEFT)
        ttk.Radiobutton(r_frame, text="父文件夹名_原文件名", variable=self.rename_mode, value="parent_name").pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(r_frame, text="自定义前缀 + 序号", variable=self.rename_mode, value="prefix").pack(side=tk.LEFT)
        
        ttk.Label(rename_frame, text="自定义前缀:").grid(row=1, column=0, sticky="w")
        ttk.Entry(rename_frame, textvariable=self.custom_prefix, width=20).grid(row=1, column=1, sticky="w")

        ttk.Label(rename_frame, text="冲突处理 (遇到重名):").grid(row=2, column=0, sticky="w")
        c_frame = ttk.Frame(rename_frame)
        c_frame.grid(row=2, column=1, sticky="w", columnspan=2)
        ttk.Radiobutton(c_frame, text="自动重命名 (例如 file (1).txt)", variable=self.conflict_mode, value="auto_rename").pack(side=tk.LEFT)
        ttk.Radiobutton(c_frame, text="跳过", variable=self.conflict_mode, value="skip").pack(side=tk.LEFT, padx=10)
        ttk.Radiobutton(c_frame, text="覆盖 (危险!)", variable=self.conflict_mode, value="overwrite").pack(side=tk.LEFT)

        # 4. 控制与进度
        ctrl_frame = ttk.Frame(main_frame, padding="5")
        ctrl_frame.pack(fill=tk.X, pady=10)

        self.btn_start = ttk.Button(ctrl_frame, text="开始合并", command=self.start_task)
        self.btn_start.pack(side=tk.LEFT, padx=5)
        
        self.btn_stop = ttk.Button(ctrl_frame, text="停止", command=self.stop_task, state=tk.DISABLED)
        self.btn_stop.pack(side=tk.LEFT, padx=5)

        ttk.Label(ctrl_frame, textvariable=self.status_var).pack(side=tk.LEFT, padx=20)

        self.progress = ttk.Progressbar(main_frame, variable=self.progress_var, maximum=100)
        self.progress.pack(fill=tk.X, padx=5, pady=5)

        # 5. 日志区域
        log_frame = ttk.LabelFrame(main_frame, text="运行日志", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)
        
        self.log_text = tk.Text(log_frame, height=10, state=tk.DISABLED)
        scrollbar = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        self.log_text.configure(yscrollcommand=scrollbar.set)
        
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    def log(self, msg):
        self.root.after(0, self._append_log, msg)

    def _append_log(self, msg):
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.insert(tk.END, msg + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state=tk.DISABLED)

    def browse_source(self):
        path = filedialog.askdirectory()
        if path:
            self.source_dir.set(path)
            # 默认目标文件夹和源文件夹相同
            self.target_dir.set(path)
            self.save_config()

    def browse_target(self):
        path = filedialog.askdirectory()
        if path:
            self.target_dir.set(path)
            self.save_config()

    def update_progress(self, stage, current, total):
        if stage == "scanning":
            self.root.after(0, lambda: self.status_var.set(f"正在扫描文件... 已找到 {current} 个"))
        elif stage == "processing":
            percent = (current / total) * 100 if total > 0 else 0
            self.root.after(0, lambda: self.progress_var.set(percent))
            self.root.after(0, lambda: self.status_var.set(f"正在处理: {current}/{total} ({percent:.1f}%)"))
        elif stage == "done":
            self.root.after(0, lambda: self.progress_var.set(100))
            self.root.after(0, lambda: self.status_var.set("完成!"))
            self.root.after(0, self.task_finished)

    def start_task(self):
        source = self.source_dir.get()
        target = self.target_dir.get()
        
        if not source or not os.path.exists(source):
            messagebox.showerror("错误", "请选择有效的源文件夹")
            return
        if not target:
            messagebox.showerror("错误", "请选择目标文件夹")
            return
        
        # 启动前保存当前配置
        self.save_config()

        if self.op_mode.get() == "move":
            if not messagebox.askyesno("确认", "您选择了【移动】模式，源文件将被移动且源文件夹结构将被删除。\n确认继续吗？"):
                return

        config = {
            'source_dir': source,
            'target_parent': target,
            'files_per_folder': self.files_per_folder.get(),
            'operation_mode': self.op_mode.get(),
            'rename_mode': self.rename_mode.get(),
            'conflict_mode': self.conflict_mode.get(),
            'custom_prefix': self.custom_prefix.get()
        }

        self.btn_start.config(state=tk.DISABLED)
        self.btn_stop.config(state=tk.NORMAL)
        self.log_text.configure(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.configure(state=tk.DISABLED)
        
        self.core = MergerCore(update_callback=self.update_progress, log_callback=self.log)
        self.thread = threading.Thread(target=self.core.process, args=(config,))
        self.thread.daemon = True
        self.thread.start()

    def stop_task(self):
        if self.core:
            self.core.stop_flag = True
            self.log("正在停止...")
            self.btn_stop.config(state=tk.DISABLED)

    def task_finished(self):
        self.btn_start.config(state=tk.NORMAL)
        self.btn_stop.config(state=tk.DISABLED)
        messagebox.showinfo("完成", "任务执行完毕")

if __name__ == "__main__":
    root = tk.Tk()
    app = App(root)
    root.mainloop()
