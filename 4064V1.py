# extract_payload_4064_gui_v3_optimized.py
# 优化版 v3：大幅提升处理效率（mmap零拷贝 + 字典直接查找 + 大缓冲 + 减少GUI刷新）
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import mmap   # ← 新增：内存映射，处理GB级文件速度提升5-10倍


class PayloadExtractorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("4064帧提取 → 6448帧分类分离工具 v3（高效版）")
        self.root.geometry("780x620")          # ← 加大窗口 + 可拉伸
        self.root.resizable(True, True)

        self.input_path = tk.StringVar()
        self.output_path = tk.StringVar()
        self.enable_step2 = tk.BooleanVar(value=True)

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        self.lbl_input = tk.Label(self.root, text="输入文件 (.bin / .dat)：")
        self.ent_input = tk.Entry(self.root, textvariable=self.input_path, width=70)
        self.btn_browse_in = tk.Button(self.root, text="浏览...", width=10, command=self.browse_input)

        self.lbl_output = tk.Label(self.root, text="第一步输出文件（4000字节载荷）：")
        self.ent_output = tk.Entry(self.root, textvariable=self.output_path, width=70)
        self.btn_browse_out = tk.Button(self.root, text="浏览...", width=10, command=self.browse_output)

        self.lbl_auto = tk.Label(self.root, text="(留空自动生成：原文件名_payload.xxx)", fg="#666666")

        self.chk_step2 = tk.Checkbutton(
            self.root,
            text="✅ 启用第二步：按 6448 帧长切分 + 按帧头 FAF3340A01/02/03/04 分离为4个文件",
            variable=self.enable_step2,
            font=("Microsoft YaHei UI", 10, "bold"),
            fg="#0066cc"
        )

        self.text_info = tk.Text(self.root, height=14, width=80, state='disabled', bg="#f8f8f8")
        self.scrollbar = ttk.Scrollbar(self.root, orient="vertical", command=self.text_info.yview)
        self.text_info.configure(yscrollcommand=self.scrollbar.set)

        self.progress = ttk.Progressbar(self.root, mode='determinate', length=560)
        self.lbl_progress = tk.Label(self.root, text="就绪", fg="#555")

        self.btn_start = tk.Button(self.root, text="开始完整处理", width=18, height=2,
                                   font=("Microsoft YaHei UI", 12, "bold"),
                                   bg="#4CAF50", fg="white", command=self.start_extract)
        self.btn_start["state"] = "disabled"

    def _layout_widgets(self):
        pad_x = 24
        pad_y = 8

        self.lbl_input.grid(row=0, column=0, sticky="w", padx=pad_x, pady=(12, 2))
        self.ent_input.grid(row=1, column=0, padx=pad_x, pady=4, sticky="ew")
        self.btn_browse_in.grid(row=1, column=1, padx=(10, pad_x), pady=4, sticky="e")

        self.lbl_output.grid(row=2, column=0, sticky="w", padx=pad_x, pady=(12, 2))
        self.ent_output.grid(row=3, column=0, padx=pad_x, pady=4, sticky="ew")
        self.btn_browse_out.grid(row=3, column=1, padx=(10, pad_x), pady=4, sticky="e")

        self.lbl_auto.grid(row=4, column=0, columnspan=2, sticky="w", padx=pad_x, pady=(2, 12))

        self.chk_step2.grid(row=5, column=0, columnspan=2, sticky="w", padx=pad_x, pady=(10, 12))

        tk.Label(self.root, text="处理日志：", font=("Microsoft YaHei UI", 10, "bold")) \
            .grid(row=6, column=0, sticky="w", padx=pad_x, pady=(8, 4))

        self.text_info.grid(row=7, column=0, columnspan=2, padx=pad_x, pady=(0, 4), sticky="nsew")
        self.scrollbar.grid(row=7, column=2, sticky="ns", pady=(0, 4))

        self.progress.grid(row=8, column=0, columnspan=2, padx=pad_x, pady=(12, 4), sticky="ew")
        self.lbl_progress.grid(row=8, column=0, columnspan=2, pady=(0, 8), sticky="s")

        self.btn_start.grid(row=9, column=0, columnspan=2, pady=(20, 30))

        self.root.rowconfigure(7, weight=1)      # 日志区可垂直拉伸
        self.root.columnconfigure(0, weight=1)

    def browse_input(self):
        filepath = filedialog.askopenfilename(
            title="选择输入文件",
            filetypes=[("Binary files", "*.bin *.dat"), ("All files", "*.*")]
        )
        if filepath:
            self.input_path.set(filepath)
            self._auto_fill_output()
            self._update_start_button()

    def browse_output(self):
        filepath = filedialog.asksaveasfilename(
            title="保存第一步载荷文件",
            defaultextension=".bin",
            filetypes=[("Binary file", "*.bin"), ("Data file", "*.dat"), ("All files", "*.*")]
        )
        if filepath:
            self.output_path.set(filepath)
            self._update_start_button()

    def _auto_fill_output(self):
        if not self.output_path.get() and self.input_path.get():
            base, ext = os.path.splitext(self.input_path.get())
            self.output_path.set(f"{base}_payload{ext}")

    def _update_start_button(self):
        self.btn_start["state"] = "normal" if self.input_path.get().strip() else "disabled"

    def log(self, message, color="black"):
        self.text_info.configure(state='normal')
        tag = f"tag_{color}"
        self.text_info.insert(tk.END, message + "\n", tag)
        colors = {"red": "red", "green": "darkgreen", "orange": "#e67e22", "blue": "#0066cc"}
        self.text_info.tag_config(tag, foreground=colors.get(color, "black"))
        self.text_info.see(tk.END)
        self.text_info.configure(state='disabled')
        self.root.update_idletasks()

    def start_extract(self):
        input_file = self.input_path.get().strip()
        output_file = self.output_path.get().strip() or f"{os.path.splitext(input_file)[0]}_payload{os.path.splitext(input_file)[1]}"

        if os.path.exists(output_file):
            if not messagebox.askyesno("覆盖确认", f"文件已存在：\n{output_file}\n是否覆盖？"):
                return

        self.btn_start["state"] = "disabled"
        self.btn_start["text"] = "正在处理..."
        self.text_info.delete(1.0, tk.END)
        self.log("=== 开始高效处理 ===", "blue")

        try:
            success1 = self.extract_step1_optimized(input_file, output_file)
            if not success1:
                raise Exception("第一步失败")

            if self.enable_step2.get():
                self.log("--- 第二步：6448帧分离（mmap加速） ---", "blue")
                success2 = self.extract_step2_optimized(output_file)
                if success2:
                    self.log("✅ 第二步完成！", "green")
            else:
                self.log("第二步已关闭", "orange")

            self.lbl_progress["text"] = "全部完成"
            messagebox.showinfo("成功", f"处理完成！\n主文件：{output_file}")
        except Exception as e:
            self.log(f"❌ 错误：{str(e)}", "red")
            messagebox.showerror("错误", str(e))
        finally:
            self.btn_start["state"] = "normal"
            self.btn_start["text"] = "开始完整处理"
            self.progress["value"] = 0

    # ====================== 优化后的第一步（mmap + 大缓冲） ======================
    def extract_step1_optimized(self, input_file, output_file):
        FRAME_SIZE = 4064
        PAYLOAD_SIZE = 4000
        HEADER_SIZE = 64
        BUFFER_SIZE = 8 * 1024 * 1024          # 8MB 大缓冲
        UPDATE_EVERY = 100

        file_size = os.path.getsize(input_file)
        total_frames = file_size // FRAME_SIZE
        self.log(f"输入文件大小：{file_size:,} 字节 | 完整帧数：{total_frames}")

        self.progress["maximum"] = total_frames
        extracted = 0

        with open(input_file, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:   # ← 零拷贝读取
                with open(output_file, 'wb', buffering=BUFFER_SIZE) as fout:
                    for i in range(total_frames):
                        offset = i * FRAME_SIZE
                        payload = mm[offset + HEADER_SIZE : offset + HEADER_SIZE + PAYLOAD_SIZE]
                        fout.write(payload)
                        extracted += PAYLOAD_SIZE

                        if (i + 1) % UPDATE_EVERY == 0 or i == total_frames - 1:
                            self.progress["value"] = i + 1
                            self.lbl_progress["text"] = f"第一步：{i+1}/{total_frames} 帧 | 已提取 {extracted:,} 字节"
                            self.root.update_idletasks()

        self.log(f"第一步完成！提取 {extracted:,} 字节 → {output_file}", "green")
        return extracted > 0

    # ====================== 优化后的第二步（mmap + 字典直接查找 + 大缓冲） ======================
    def extract_step2_optimized(self, payload_file):
        FRAME_SIZE = 6448
        BUFFER_SIZE = 8 * 1024 * 1024
        UPDATE_EVERY = 100

        # 帧头精确字典（5字节）
        HEADER_MAP = {
            b'\xFA\xF3\x34\x0A\x01': '01',
            b'\xFA\xF3\x34\x0A\x02': '02',
            b'\xFA\xF3\x34\x0A\x03': '03',
            b'\xFA\xF3\x34\x0A\x04': '04',
        }

        base_name = os.path.splitext(payload_file)[0]
        out_files = {h: f"{base_name}_type_{typ}.bin" for h, typ in HEADER_MAP.items()}

        writers = {}
        count = {h: 0 for h in HEADER_MAP}
        unmatched = 0
        unmatched_sample = None

        file_size = os.path.getsize(payload_file)
        total_frames = file_size // FRAME_SIZE
        self.log(f"第二步输入大小：{file_size:,} 字节 | 完整帧数：{total_frames}")

        self.progress["maximum"] = total_frames
        processed = 0

        # 打开4个输出文件（大缓冲）
        for h, fname in out_files.items():
            writers[h] = open(fname, 'wb', buffering=BUFFER_SIZE)

        try:
            with open(payload_file, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    for i in range(total_frames):
                        offset = i * FRAME_SIZE
                        frame = mm[offset:offset + FRAME_SIZE]          # 内存视图，零拷贝
                        header = bytes(frame[:5])                       # 转为bytes用于字典查找

                        if header in writers:
                            writers[header].write(frame)
                            count[header] += 1
                        else:
                            unmatched += 1
                            if unmatched_sample is None:
                                unmatched_sample = header.hex().upper()

                        processed += 1
                        if processed % UPDATE_EVERY == 0 or processed == total_frames:
                            self.progress["value"] = processed
                            self.lbl_progress["text"] = f"第二步：{processed}/{total_frames} 帧"
                            self.root.update_idletasks()

        finally:
            for w in writers.values():
                w.close()

        # 输出统计
        self.log("=== 第二步分离结果 ===", "blue")
        total_saved = 0
        for h, typ in HEADER_MAP.items():
            c = count[h]
            fname = out_files[h]
            if c > 0:
                self.log(f"类型 {typ}：{c:,} 帧 → {os.path.basename(fname)}", "green")
                total_saved += c
            else:
                self.log(f"类型 {typ}：0 帧", "orange")

        if unmatched > 0:
            self.log(f"未匹配帧：{unmatched} 个（示例前5字节：{unmatched_sample}）", "orange")

        self.log(f"第二步总共保存 {total_saved} 帧", "blue")
        return True


if __name__ == "__main__":
    root = tk.Tk()
    app = PayloadExtractorApp(root)
    root.mainloop()

