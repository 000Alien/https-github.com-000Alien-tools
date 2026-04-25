import tkinter as tk
from tkinter import ttk, filedialog, scrolledtext
import struct
import os
import threading


class FrameAnalyzerApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("多文件帧计数分析工具")
        self.geometry("1000x800")
        self.create_widgets()
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.running = False
        self.current_file_progress_max = 0

    def create_widgets(self):
        # 功能说明模块
        self.description = scrolledtext.ScrolledText(self, height=8)
        self.description.pack(fill=tk.X, padx=10, pady=5)
        self.description.insert(tk.END, """功能说明：
1. 支持批量分析BIN/DAT格式文件
2. 自动识别帧结构：起始标志(1ACFFC1D) + 帧计数(4字节) + 递增码(4088字节)
3. 检查帧计数连续性并输出详细结果
4. 实时显示双重进度（文件进度和帧进度）
5. 自动保存分析结果到原文件目录（[原文件名PD.txt]）
6. 支持添加/移除分析文件""")
        self.description.config(state=tk.DISABLED)

        # 文件管理模块
        self.file_frame = ttk.LabelFrame(self, text="待分析文件列表")
        self.file_frame.pack(fill=tk.BOTH, padx=10, pady=5, expand=True)

        self.file_listbox = tk.Listbox(self.file_frame, selectmode=tk.EXTENDED, height=6)
        self.scrollbar = ttk.Scrollbar(self.file_frame, orient=tk.VERTICAL, command=self.file_listbox.yview)
        self.file_listbox.configure(yscrollcommand=self.scrollbar.set)
        self.file_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        self.btn_frame = ttk.Frame(self.file_frame)
        self.btn_frame.pack(side=tk.RIGHT, fill=tk.Y, padx=5)

        self.add_btn = ttk.Button(self.btn_frame, text="添加文件", command=self.add_files)
        self.add_btn.pack(pady=2, fill=tk.X)

        self.remove_btn = ttk.Button(self.btn_frame, text="移除选中", command=self.remove_files)
        self.remove_btn.pack(pady=2, fill=tk.X)

        self.clear_btn = ttk.Button(self.btn_frame, text="清空列表", command=self.clear_files)
        self.clear_btn.pack(pady=2, fill=tk.X)

        # 进度显示模块
        self.progress_frame = ttk.Frame(self)
        self.progress_frame.pack(fill=tk.X, padx=10, pady=5)

        self.file_progress = ttk.Progressbar(self.progress_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.file_progress.pack(fill=tk.X, pady=2)

        self.frame_progress = ttk.Progressbar(self.progress_frame, orient=tk.HORIZONTAL, mode='determinate')
        self.frame_progress.pack(fill=tk.X, pady=2)

        self.status_label = ttk.Label(self.progress_frame, text="就绪")
        self.status_label.pack(fill=tk.X)

        # 控制按钮
        self.control_frame = ttk.Frame(self)
        self.control_frame.pack(pady=5)
        self.start_btn = ttk.Button(self.control_frame, text="开始分析", command=self.start_analysis)
        self.start_btn.pack(side=tk.LEFT, padx=5)
        self.stop_btn = ttk.Button(self.control_frame, text="停止分析", command=self.stop_analysis)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        # 结果展示
        self.result_area = scrolledtext.ScrolledText(self)
        self.result_area.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

    def add_files(self):
        file_paths = filedialog.askopenfilenames(filetypes=[("Binary Files", "*.bin;*.dat")])
        if file_paths:
            for path in file_paths:
                if path not in self.file_listbox.get(0, tk.END):
                    self.file_listbox.insert(tk.END, path)

    def remove_files(self):
        selected = self.file_listbox.curselection()
        for index in reversed(selected):
            self.file_listbox.delete(index)

    def clear_files(self):
        self.file_listbox.delete(0, tk.END)

    def start_analysis(self):
        if self.running:
            return

        file_paths = self.file_listbox.get(0, tk.END)
        if not file_paths:
            self.show_message("错误：请先添加要分析的文件！")
            return

        self.running = True
        self.start_btn.config(state=tk.DISABLED)
        self.result_area.config(state=tk.NORMAL)
        self.result_area.delete(1.0, tk.END)
        self.file_progress["value"] = 0
        self.frame_progress["value"] = 0
        self.status_label.config(text="准备开始分析...")

        analysis_thread = threading.Thread(target=self.analyze_files, args=(file_paths,))
        analysis_thread.start()

    def stop_analysis(self):
        if self.running:
            self.running = False
            self.show_message("\n用户请求停止分析...")

    def analyze_files(self, file_paths):
        total_files = len(file_paths)
        self.file_progress.config(maximum=total_files)

        for file_index, file_path in enumerate(file_paths):
            if not self.running:
                break

            self.update_status(f"正在分析文件 ({file_index + 1}/{total_files}): {os.path.basename(file_path)}")
            self.file_progress["value"] = file_index + 1
            self.analyze_single_file(file_path, file_index + 1, total_files)

        if self.running:
            self.show_message("\n所有文件分析完成！")
            self.status_label.config(text="分析完成")
        else:
            self.show_message("\n分析已中止！")
            self.status_label.config(text="分析中止")

        self.running = False
        self.start_btn.config(state=tk.NORMAL)

    def analyze_single_file(self, file_path, file_num, total_files):
        HEADER = b'\x1a\xcf\xfc\x1d'
        FRAME_SIZE = 4096
        error_count = 0
        previous_count = None
        total_frames = 0
        log_content = []

        try:
            file_size = os.path.getsize(file_path)
            total_frames = file_size // FRAME_SIZE
            remaining_bytes = file_size % FRAME_SIZE

            self.frame_progress.config(maximum=total_frames, value=0)
            log_content.append(f"\n{'=' * 40}")
            log_content.append(f"文件 {file_num}/{total_files}: {file_path}")
            log_content.append(f"总帧数：{total_frames}")

            if remaining_bytes > 0:
                log_content.append(f"警告：文件末尾存在{remaining_bytes}字节不完整数据！")

            with open(file_path, 'rb') as f:
                for frame_index in range(total_frames):
                    if not self.running:
                        break

                    chunk = f.read(FRAME_SIZE)
                    if len(chunk) != FRAME_SIZE:
                        break

                    header = chunk[:4]
                    if header != HEADER:
                        log_content.append(f"帧 {frame_index}: 无效帧头！")
                        error_count += 1
                        continue

                    try:
                        frame_count = struct.unpack('>I', chunk[4:8])[0]
                    except:
                        log_content.append(f"帧 {frame_index}: 帧计数解析失败！")
                        error_count += 1
                        continue

                    if previous_count is not None:
                        if frame_count != previous_count + 1:
                            log_content.append(
                                f"帧 {frame_index} 不连续: 当前计数 {frame_count}, 期望计数 {previous_count + 1}")
                            error_count += 1
                    previous_count = frame_count

                    self.frame_progress["value"] = frame_index + 1

            log_content.append("\n分析结果：")
            log_content.append(f"总帧数: {total_frames}")
            log_content.append(f"错误数量: {error_count}")
            if total_frames > 0:
                integrity = (total_frames - error_count) / total_frames
                log_content.append(f"数据完整性: {integrity:.2%}")
            else:
                log_content.append("数据完整性: 无有效帧")

            output_path = self.generate_output_path(file_path)
            self.save_result(output_path, "\n".join(log_content))
            log_content.append(f"\n结果已保存至：{output_path}")
            log_content.append('=' * 40)

            self.show_message("\n".join(log_content))

        except Exception as e:
            error_msg = f"处理文件 {file_path} 时发生错误: {str(e)}"
            self.show_message(error_msg)

    def generate_output_path(self, origin_path):
        filename = os.path.basename(origin_path)
        file_dir = os.path.dirname(origin_path)
        name_without_ext = os.path.splitext(filename)[0]
        return os.path.join(file_dir, f"{name_without_ext}PD.txt")

    def save_result(self, output_path, content):
        try:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(content)
        except Exception as e:
            self.show_message(f"保存失败：{output_path}\n错误：{str(e)}")

    def update_status(self, message):
        self.status_label.config(text=message)
        self.update_idletasks()

    def show_message(self, message):
        self.result_area.insert(tk.END, message + "\n")
        self.result_area.see(tk.END)
        self.result_area.config(state=tk.DISABLED)
        self.update_idletasks()

    def on_close(self):
        self.running = False
        self.destroy()


if __name__ == "__main__":
    app = FrameAnalyzerApp()
    app.mainloop()