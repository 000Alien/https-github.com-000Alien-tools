import re
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from pathlib import Path
from typing import Dict, List


class BinDatProcessor:
    def __init__(self):
        self.window = tk.Tk()
        self.window.title("BIN/DAT 数据处理工具")
        self.window.geometry("600x450")

        # 模式配置（每个XX表示1个字节的通配符）
        self.patterns = [
            {
                "name": "TypeA",
                "pattern": "0897....00F9",  # 08 97 XX XX 00 F9
                "length": 256
            },
            {
                "name": "TypeB",
                "pattern": "0870....0389",  # 08 70 XX XX 03 89
                "length": 1024
            }
        ]

        self.create_widgets()
        self.input_file = ""

    def create_widgets(self):
        """创建界面组件"""
        # 文件选择区域
        frame_top = tk.Frame(self.window)
        frame_top.pack(pady=10)

        self.btn_select = tk.Button(
            frame_top,
            text="选择BIN/DAT文件",
            command=self.select_file,
            width=20
        )
        self.btn_select.pack(side=tk.LEFT, padx=5)

        self.lbl_file = tk.Label(frame_top, text="未选择文件")
        self.lbl_file.pack(side=tk.LEFT)

        # 进度条
        self.progress = ttk.Progressbar(
            self.window,
            orient=tk.HORIZONTAL,
            length=400,
            mode='determinate'
        )
        self.progress.pack(pady=5)

        # 处理按钮
        self.btn_process = tk.Button(
            self.window,
            text="开始处理",
            command=self.process_file,
            state=tk.DISABLED,
            width=15
        )
        self.btn_process.pack(pady=10)

        # 日志输出
        self.log_text = tk.Text(self.window, height=12, width=70)
        self.log_text.pack(pady=10)

        # 配置样式
        self.window.option_add('*Font', '微软雅黑 10')

    def select_file(self):
        """选择文件"""
        file_path = filedialog.askopenfilename(
            filetypes=[("Binary Files", "*.bin *.dat"), ("All Files", "*.*")]
        )
        if file_path:
            self.input_file = file_path
            self.lbl_file.config(text=Path(file_path).name)
            self.btn_process.config(state=tk.NORMAL)
            self.log(f"已选择文件：{file_path}")

    def pattern_to_regex(self, pattern: str) -> bytes:
        """将模式字符串转换为正则表达式字节模式"""
        byte_pattern = bytearray()
        # 每两个字符分割处理（每个字节用2个字符表示）
        parts = [pattern[i:i+2] for i in range(0, len(pattern), 2)]

        for part in parts:
            if part == "..":
                byte_pattern.append(46)  # 通配符'.'
            else:
                try:
                    byte_pattern.append(int(part, 16))
                except ValueError:
                    raise ValueError(f"无效的十六进制值: {part}")

        return bytes(byte_pattern)

    def process_file(self):
        """处理文件"""
        try:
            self.btn_process.config(state=tk.DISABLED)
            self.progress['value'] = 0
            self.window.update()

            # 准备输出目录
            output_dir = Path(self.input_file).parent / "extracted_data"
            output_dir.mkdir(exist_ok=True)

            with open(self.input_file, 'rb') as f:
                data = f.read()

            total_size = len(data)
            results: Dict[str, List[bytes]] = {p["name"]: [] for p in self.patterns}

            # 处理每个模式
            for pattern_idx, pattern in enumerate(self.patterns):
                self.log(f"正在扫描 {pattern['name']} 模式...")
                regex_pattern = self.pattern_to_regex(pattern["pattern"])
                regex = re.compile(regex_pattern)

                # 进度计算
                progress_per_pattern = 100 / len(self.patterns)
                base_progress = pattern_idx * progress_per_pattern

                # 查找所有匹配
                matches = list(regex.finditer(data))
                for match_idx, match in enumerate(matches, 1):
                    start = match.start()
                    expected_length = pattern["length"]
                    available_length = len(data) - start
                    actual_length = min(expected_length, available_length)

                    # 记录不足长度的情况
                    if actual_length < expected_length:
                        self.log(
                            f"警告：在 0x{start:X} 发现 {pattern['name']} 数据头，"
                            f"实际长度 {actual_length} 字节（不足 {expected_length} 字节）"
                        )

                    # 提取数据块
                    block = data[start:start+actual_length]
                    results[pattern["name"]].append(block)

                    # 更新进度
                    current_progress = base_progress + (match_idx/len(matches)) * progress_per_pattern
                    self.progress['value'] = current_progress
                    self.window.update_idletasks()

            # 保存结果到两个文件
            total_blocks = 0
            for pattern in self.patterns:
                p_name = pattern["name"]
                blocks = results[p_name]
                if not blocks:
                    continue

                output_path = output_dir / f"{p_name}.bin"
                with open(output_path, 'wb') as f:
                    for block in blocks:
                        f.write(block)

                self.log(f"已写入 {len(blocks)} 个 {p_name} 数据块到：{output_path}")
                total_blocks += len(blocks)

            self.progress['value'] = 100
            self.log(f"处理完成！共提取 {total_blocks} 个数据块")
            messagebox.showinfo("完成", f"数据已保存到：\n{output_dir}")

        except Exception as e:
            messagebox.showerror("错误", str(e))
        finally:
            self.btn_process.config(state=tk.NORMAL)

    def log(self, message: str):
        """记录日志"""
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.window.update_idletasks()

    def run(self):
        self.window.mainloop()


if __name__ == "__main__":
    app = BinDatProcessor()
    app.run()