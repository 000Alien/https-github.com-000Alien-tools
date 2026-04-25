#延时遥测通过1553回放
import os
import struct
import threading
import datetime
import subprocess
import sys
from tkinter import Tk, filedialog, Text, Button, Label, ttk, messagebox, Scrollbar, END, StringVar, Entry

START_0870 = b'\x08\x70'
END_0870 = b'\x01\xB5'
START_0897 = b'\x08\x97'
FRAME_0897_LEN = 256

class FrameExtractorGUI:
    def __init__(self, master):
        self.master = master
        master.title("BIN/DAT 帧提取工具")

        self.label = Label(master, text="选择 BIN 或 DAT 文件：")
        self.label.pack()

        self.browse_button = Button(master, text="浏览文件", command=self.browse_file)
        self.browse_button.pack()

        self.output_label = Label(master, text="选择输出目录：")
        self.output_label.pack()

        self.output_dir_var = StringVar()
        self.output_entry = Entry(master, textvariable=self.output_dir_var, width=60)
        self.output_entry.pack()
        self.output_button = Button(master, text="选择目录", command=self.select_output_dir)
        self.output_button.pack()

        self.start_button = Button(master, text="开始处理", command=self.start_processing)
        self.start_button.pack()

        self.progress = ttk.Progressbar(master, length=300, mode='determinate')
        self.progress.pack()

        self.log_text = Text(master, height=15, width=80)
        self.log_text.pack()

        self.scrollbar = Scrollbar(master, command=self.log_text.yview)
        self.scrollbar.pack(side='right', fill='y')
        self.log_text.config(yscrollcommand=self.scrollbar.set)

        self.file_path = None
        self.output_dir = os.getcwd()
        self.output_dir_var.set(self.output_dir)
        self.log_file = None

    def browse_file(self):
        self.file_path = filedialog.askopenfilename(filetypes=[("Binary files", "*.bin;*.dat")])
        self.log(f"已选择文件：{self.file_path}")

    def select_output_dir(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir = directory
            self.output_dir_var.set(directory)
            self.log(f"输出目录设为：{directory}")

    def log(self, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {message}"
        self.log_text.insert(END, line + '\n')
        self.log_text.see(END)
        if self.log_file:
            self.log_file.write(line + '\n')

    def start_processing(self):
        if not self.file_path:
            messagebox.showwarning("未选择文件", "请先选择一个BIN或DAT文件！")
            return
        threading.Thread(target=self.process_file).start()

    def process_file(self):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        log_path = os.path.join(self.output_dir, f"processing_log_{timestamp}.txt")
        self.log_file = open(log_path, 'w', encoding='utf-8')
        self.log(f"日志文件：{log_path}")

        self.progress['value'] = 0
        with open(self.file_path, 'rb') as f:
            data = f.read()

        total_len = len(data)
        self.log(f"文件长度：{total_len} 字节")

        # 提取0870帧
        frame0870_list = []
        idx = 0
        while idx < total_len - 4:
            if data[idx:idx+2] == START_0870:
                frame_count = struct.unpack('>H', data[idx+2:idx+4])[0]
                end_idx = data.find(END_0870, idx+4)
                if end_idx != -1:
                    frame = data[idx:end_idx+2]
                    frame0870_list.append((frame_count, frame))
                    idx = end_idx + 2
                else:
                    idx += 2
            else:
                idx += 1
        frame0870_list.sort()
        output_path_0870 = os.path.join(self.output_dir, f"0870_output_{timestamp}.bin")
        with open(output_path_0870, 'wb') as f:
            for _, frame in frame0870_list:
                f.write(frame)
        lost_0870 = self.check_frame_sequence([c for c, _ in frame0870_list])
        self.log(f"提取0870帧共 {len(frame0870_list)} 条，帧计数异常：{lost_0870}")

        # 提取0897帧
        frame0897_list = []
        idx = 0
        while idx < total_len - FRAME_0897_LEN:
            if data[idx:idx+2] == START_0897:
                frame_count = struct.unpack('>H', data[idx+2:idx+4])[0]
                frame = data[idx:idx+FRAME_0897_LEN]
                frame0897_list.append((frame_count, frame))
                idx += FRAME_0897_LEN
            else:
                idx += 1
        frame0897_list.sort()
        output_path_0897 = os.path.join(self.output_dir, f"0897_output_{timestamp}.bin")
        with open(output_path_0897, 'wb') as f:
            for _, frame in frame0897_list:
                f.write(frame)
        lost_0897 = self.check_frame_sequence([c for c, _ in frame0897_list])
        self.log(f"提取0897帧共 {len(frame0897_list)} 条，帧计数异常：{lost_0897}")

        self.progress['value'] = 100
        self.log(f"处理完成！输出文件：\n{output_path_0870}\n{output_path_0897}")

        # 自动打开输出目录
        try:
            if sys.platform.startswith('win'):
                os.startfile(self.output_dir)
            elif sys.platform.startswith('darwin'):
                subprocess.call(['open', self.output_dir])
            else:
                subprocess.call(['xdg-open', self.output_dir])
        except Exception as e:
            self.log(f"无法自动打开目录: {e}")

        # 提示声音与弹窗
        try:
            self.master.bell()
        except:
            pass
        messagebox.showinfo("处理完成", "帧提取已完成并保存到指定目录！")

        if self.log_file:
            self.log_file.close()
            self.log_file = None

    def check_frame_sequence(self, seq_list):
        # 判断连续性并压缩重复错误
        errors = []
        for i in range(1, len(seq_list)):
            if seq_list[i] != seq_list[i-1] + 1:
                errors.append(f"{seq_list[i-1]}->{seq_list[i]}")
        if not errors:
            return []
        # 收集每个错误出现的索引位置
        error_positions = {}
        for idx, err in enumerate(errors):
            error_positions.setdefault(err, []).append(idx)
        compressed = []
        for err, poses in error_positions.items():
            if len(poses) == 1:
                compressed.append(err)
            else:
                first = poses[0]
                last = poses[-1]
                compressed.append(f"{err} (首次索引{first}, 最后索引{last})")
        return compressed

if __name__ == '__main__':
    root = Tk()
    app = FrameExtractorGUI(root)
    root.mainloop()
