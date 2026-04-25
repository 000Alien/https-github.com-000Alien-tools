#应急记录器数据通过数据通过应急地检回放20250820
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from collections import defaultdict
import datetime
import os
import queue
import threading
import struct

INFO_TEXT = """
功能说明：
1. 支持解析以 0xEB9090EB 为同步头的数据帧；
2. 特殊处理0859类型帧：提取896字节数据（含同步头）；
3. 所有帧按类型分类保存为 .bin 文件；
4. 对所有类型的帧计数连续性进行检查；
5. 检查结果按类型分文件保存在 output/check 文件夹中；
6. 若帧计数不连续，输出具体丢失帧段说明并优先记录在日志顶部；
7. 提供图形界面、日志输出、进度条显示。
8. 089D APID遥测协议不符合SPP包计数协议，故帧计数有效性不能判读。
"""

def extract_frames(data):
    sync_pattern = b'\xEB\x90\x90\xEB'
    HEADER_LEN = 20
    SPECIAL_LENGTH = 912

    frame_dict = defaultdict(list)
    seq_numbers = defaultdict(list)

    pos = 0
    data_len = len(data)
    while True:
        sync_pos = data.find(sync_pattern, pos)
        if sync_pos < 0 or sync_pos + HEADER_LEN > data_len:
            break

        next_sync = data.find(sync_pattern, sync_pos + len(sync_pattern))
        frame_start = sync_pos
        frame_end = next_sync if next_sync > 0 else data_len

        if frame_end - frame_start < HEADER_LEN:
            pos = sync_pos + len(sync_pattern)
            continue

        header = data[frame_start:frame_start + HEADER_LEN]
        type_bytes = header[16:18]
        num_bytes = header[18:20]
        byte_id = type_bytes.hex().upper()

        frame_num = struct.unpack('>H', num_bytes)[0]
        seq_numbers[byte_id].append(frame_num)

        available = frame_end - frame_start
        if byte_id == '0859':
            take_len = min(SPECIAL_LENGTH, available)
            frame_data = data[frame_start:frame_start + take_len]
        else:
            frame_data = data[frame_start:frame_end]

        frame_dict[byte_id].append(frame_data)
        pos = frame_end

    return frame_dict, seq_numbers

class EnhancedApplication(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.master.title("存储服务器记录，应急地检回放数据判断 v2.5")
        self.master.minsize(700, 600)

        self.input_path = tk.StringVar()
        self.output_dir = tk.StringVar()
        self.log_queue = queue.Queue()

        self.pack(padx=15, pady=15, fill=tk.BOTH, expand=True)
        self.create_widgets()
        self.check_queue()

    def create_widgets(self):
        # Input and Output section
        tk.Label(self, text="输入文件:").grid(row=0, column=0, sticky="w")
        tk.Entry(self, textvariable=self.input_path, width=60).grid(row=0, column=1)
        tk.Button(self, text="浏览", command=self.browse_input).grid(row=0, column=2)

        tk.Label(self, text="输出目录:").grid(row=1, column=0, sticky="w")
        tk.Entry(self, textvariable=self.output_dir, width=60).grid(row=1, column=1)
        tk.Button(self, text="浏览", command=self.browse_output).grid(row=1, column=2)

        # Progress Bar
        self.progress = ttk.Progressbar(self, orient="horizontal", length=400, mode="determinate")
        self.progress.grid(row=2, column=1, pady=10)

        # Start button
        tk.Button(
            self, text="开始解析", command=self.start_processing,
            bg="#2196F3", fg="white", font=('微软雅黑', 10, 'bold')
        ).grid(row=3, column=1, pady=5)

        # Log Text
        self.log_text = tk.Text(self, wrap=tk.WORD, height=18, width=90)
        self.log_text.grid(row=4, column=0, columnspan=3, sticky="nsew")

        scrollbar = tk.Scrollbar(self, command=self.log_text.yview)
        scrollbar.grid(row=4, column=3, sticky="ns")
        self.log_text.config(yscrollcommand=scrollbar.set)

        # Info section
        self.info_label = tk.Label(self, text=INFO_TEXT, justify="left", anchor="nw", width=80, height=10)
        self.info_label.grid(row=5, column=0, columnspan=3, pady=10, sticky="nsew")

    def browse_input(self):
        filename = filedialog.askopenfilename(
            filetypes=[("二进制文件", "*.bin")],
            title="选择要解析的BIN文件"
        )
        if filename:
            self.input_path.set(filename)

    def browse_output(self):
        directory = filedialog.askdirectory(title="选择输出目录")
        if directory:
            self.output_dir.set(directory)

    def start_processing(self):
        input_file = self.input_path.get()
        output_dir = self.output_dir.get()
        if not input_file or not output_dir:
            self.log_text.insert(tk.END, "错误：请先选择输入文件和输出目录\n")
            return
        if not os.path.exists(input_file):
            self.log_text.insert(tk.END, "错误：输入文件不存在\n")
            return

        os.makedirs(output_dir, exist_ok=True)
        threading.Thread(
            target=self.process_files,
            args=(input_file, output_dir),
            daemon=True
        ).start()

    def check_queue(self):
        while True:
            try:
                msg = self.log_queue.get_nowait()
                if isinstance(msg, tuple):
                    level, text = msg
                    if level == "error":
                        self.log_text.insert(tk.END, f"错误：{text}\n")
                    elif level == "warning":
                        self.log_text.insert(tk.END, f"警告：{text}\n")
                else:
                    self.log_text.insert(tk.END, f"{msg}\n")
                    self.log_text.see(tk.END)
                    self.write_to_log(msg)
            except queue.Empty:
                break
        self.master.after(100, self.check_queue)

    def write_to_log(self, msg):
        try:
            log_path = os.path.join(self.output_dir.get(), "processing.log")
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")
        except Exception as e:
            print(f"日志写入失败: {e}")

    def process_files(self, input_path, output_dir):
        try:
            self.log_queue.put(f"{'='*10} 开始处理 {'='*10}")
            with open(input_path, "rb") as f:
                data = f.read()
            self.log_queue.put(f"文件大小: {len(data)} 字节")

            frame_dict, seq_numbers = extract_frames(data)
            total_types = len(frame_dict)
            self.progress["maximum"] = 100
            self.progress["value"] = 0

            check_dir = os.path.join(output_dir, "check")
            os.makedirs(check_dir, exist_ok=True)

            warnings_first = []
            normals = []

            for idx, (byte_id, nums) in enumerate(seq_numbers.items()):
                lost_count = 0
                lost_ranges = []
                same_seq_count = 1  # 计数连续相同帧计数的帧数量
                first_frame = None
                last_frame = None

                for prev, curr in zip(nums, nums[1:]):
                    if curr - prev > 1:
                        # 如果帧之间有间隔，记录丢失的帧范围
                        lost = curr - prev - 1
                        lost_count += lost
                        lost_ranges.append(f"{prev+1}-{curr-1}")
                    elif curr == prev:  # 跟踪连续帧计数相同的情况
                        same_seq_count += 1
                    else:
                        # 如果帧计数不同，记录连续帧计数相同的第一帧和最后一帧
                        if same_seq_count > 1:
                            first_frame = prev
                            last_frame = curr
                        same_seq_count = 1  # 重置计数器，开始计数下一个序列

                # 如果有连续相同的帧，记录该段连续帧的日志
                if same_seq_count > 1:
                    if first_frame is None:  # 如果只有最后几帧相同
                        first_frame = nums[-same_seq_count]
                    last_frame = nums[-1]
                    log_line = f"类型 {byte_id}：发现相同帧计数 {first_frame} - {last_frame}，共 {same_seq_count} 帧连续"
                    warnings_first.append(f"[帧计数相同] {log_line}")
                    self.log_queue.put(("warning", log_line))

                if lost_count > 0:
                    log_line = f"类型 {byte_id}：帧计数不连续，丢失 {lost_count} 个帧（缺失段: {', '.join(lost_ranges)}）"
                    warnings_first.append(f"[帧丢失] {log_line}")
                    self.log_queue.put(("warning", log_line))
                else:
                    log_line = f"类型 {byte_id}：帧计数连续"
                    normals.append(log_line)
                    self.log_queue.put(log_line)

                with open(os.path.join(check_dir, f"{byte_id}_check.txt"), "w", encoding="utf-8") as f:
                    f.write(log_line + "\n")

                self.progress["value"] = (idx + 1) * 100 // total_types

            log_path = os.path.join(output_dir, "processing.log")
            with open(log_path, "a", encoding="utf-8") as f:
                for msg in warnings_first + normals:
                    f.write(f"[{datetime.datetime.now():%Y-%m-%d %H:%M:%S}] {msg}\n")

            for byte_id, frames in frame_dict.items():
                path = os.path.join(output_dir, f"{byte_id}.bin")
                with open(path, "wb") as f:
                    for frame in frames:
                        f.write(frame)
                self.log_queue.put(f"已生成 {byte_id}.bin ({len(frames)} 帧)")

            self.log_queue.put(f"{'='*10} 处理完成 {'='*10}")
            self.progress["value"] = 100
        except Exception as e:
            self.log_queue.put(("error", f"处理错误: {e}"))

if __name__ == "__main__":
    root = tk.Tk()
    app = EnhancedApplication(master=root)
    root.mainloop()
