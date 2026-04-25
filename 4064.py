# extract_payload_4064_gui_v4_complete.py
import os
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
import mmap


class PayloadExtractorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("4064 → 6448 帧处理工具 v4 - 完整版")
        self.root.geometry("820x680")
        self.root.resizable(True, True)

        self.input_path = tk.StringVar()             # 第一步输入文件
        self.payload_path = tk.StringVar()           # 第二步输入文件（单独执行时使用）
        self.output_path = tk.StringVar()            # 第一步输出文件（默认同目录）

        self.only_step2 = tk.BooleanVar(value=False)
        self.enable_step1 = tk.BooleanVar(value=True)
        self.enable_step2 = tk.BooleanVar(value=True)

        self._create_widgets()
        self._layout_widgets()

    def _create_widgets(self):
        # 第一步输入
        self.lbl_input = tk.Label(self.root, text="第一步输入文件 (.bin / .dat)：")
        self.ent_input = tk.Entry(self.root, textvariable=self.input_path, width=75)
        self.btn_browse_in = tk.Button(self.root, text="浏览...", command=self.browse_input1)

        # 第一步输出（默认同目录）
        self.lbl_output = tk.Label(self.root, text="第一步输出（载荷文件）：")
        self.ent_output = tk.Entry(self.root, textvariable=self.output_path, width=75)
        self.btn_browse_out = tk.Button(self.root, text="浏览...", command=self.browse_output)

        # 第二步输入
        self.lbl_payload_in = tk.Label(self.root, text="第二步输入文件（已提取载荷）：")
        self.ent_payload_in = tk.Entry(self.root, textvariable=self.payload_path, width=75)
        self.btn_browse_payload = tk.Button(self.root, text="浏览...", command=self.browse_payload)

        # 开关区域
        self.chk_only_step2 = tk.Checkbutton(
            self.root, text="仅执行第二步（跳过第一步）", variable=self.only_step2,
            command=self._sync_switches, font=("Microsoft YaHei UI", 10, "bold"), fg="#d32f2f"
        )
        self.chk_step1 = tk.Checkbutton(
            self.root, text="执行第一步（4064帧 → 提取4000字节）", variable=self.enable_step1
        )
        self.chk_step2 = tk.Checkbutton(
            self.root, text="执行第二步（6448帧切分 + 按帧头分离）", variable=self.enable_step2,
            font=("Microsoft YaHei UI", 10, "bold"), fg="#0066cc"
        )

        # 日志与进度
        self.text_info = tk.Text(self.root, height=16, width=85, state='disabled', bg="#f9f9f9")
        self.scrollbar = ttk.Scrollbar(self.root, orient="vertical", command=self.text_info.yview)
        self.text_info.configure(yscrollcommand=self.scrollbar.set)

        self.progress = ttk.Progressbar(self.root, mode='determinate', length=600)
        self.lbl_progress = tk.Label(self.root, text="就绪", fg="#555")

        self.btn_start = tk.Button(self.root, text="开始处理", width=20, height=2,
                                   font=("Microsoft YaHei UI", 12, "bold"),
                                   bg="#4CAF50", fg="white", command=self.start_process)
        self.btn_start["state"] = "disabled"

    def _layout_widgets(self):
        pad_x, pad_y = 24, 8
        r = 0

        self.lbl_input.grid(row=r, column=0, sticky="w", padx=pad_x, pady=(12, 2)); r += 1
        self.ent_input.grid(row=r, column=0, padx=pad_x, pady=4, sticky="ew")
        self.btn_browse_in.grid(row=r, column=1, padx=(10, pad_x), pady=4, sticky="e"); r += 1

        self.lbl_output.grid(row=r, column=0, sticky="w", padx=pad_x, pady=(12, 2)); r += 1
        self.ent_output.grid(row=r, column=0, padx=pad_x, pady=4, sticky="ew")
        self.btn_browse_out.grid(row=r, column=1, padx=(10, pad_x), pady=4, sticky="e"); r += 1

        self.lbl_payload_in.grid(row=r, column=0, sticky="w", padx=pad_x, pady=(16, 2)); r += 1
        self.ent_payload_in.grid(row=r, column=0, padx=pad_x, pady=4, sticky="ew")
        self.btn_browse_payload.grid(row=r, column=1, padx=(10, pad_x), pady=4, sticky="e"); r += 1

        self.chk_only_step2.grid(row=r, column=0, columnspan=2, sticky="w", padx=pad_x, pady=(16, 4)); r += 1
        self.chk_step1.grid(row=r, column=0, columnspan=2, sticky="w", padx=pad_x+20, pady=2); r += 1
        self.chk_step2.grid(row=r, column=0, columnspan=2, sticky="w", padx=pad_x+20, pady=(2, 12)); r += 1

        tk.Label(self.root, text="处理日志：", font=("Microsoft YaHei UI", 10, "bold")) \
            .grid(row=r, column=0, sticky="w", padx=pad_x, pady=(8, 4)); r += 1

        self.text_info.grid(row=r, column=0, columnspan=2, padx=pad_x, pady=(0, 4), sticky="nsew")
        self.scrollbar.grid(row=r, column=2, sticky="ns", pady=(0, 4)); r += 1

        self.progress.grid(row=r, column=0, columnspan=2, padx=pad_x, pady=(12, 4), sticky="ew")
        self.lbl_progress.grid(row=r, column=0, columnspan=2, pady=(0, 8), sticky="s"); r += 1

        self.btn_start.grid(row=r, column=0, columnspan=2, pady=(20, 30))

        self.root.rowconfigure(r-3, weight=1)   # 日志区可扩展
        self.root.columnconfigure(0, weight=1)

    def _sync_switches(self):
        if self.only_step2.get():
            self.enable_step1.set(False)
            self.chk_step1["state"] = "disabled"
            self.chk_step1["text"] = "第一步已禁用（仅第二步）"
        else:
            self.chk_step1["state"] = "normal"
            self.chk_step1["text"] = "执行第一步（4064帧 → 提取4000字节）"

    def browse_input1(self):
        path = filedialog.askopenfilename(filetypes=[("Binary files", "*.bin *.dat"), ("All files", "*.*")])
        if path:
            self.input_path.set(path)
            # 自动建议输出到同一目录
            if not self.output_path.get():
                dirname = os.path.dirname(path)
                base, ext = os.path.splitext(os.path.basename(path))
                suggested = os.path.join(dirname, f"{base}_payload{ext}")
                self.output_path.set(suggested)
            self._update_start_button()

    def browse_output(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".bin",
            filetypes=[("Binary file", "*.bin"), ("Data file", "*.dat"), ("All files", "*.*")]
        )
        if path:
            self.output_path.set(path)
            self._update_start_button()

    def browse_payload(self):
        path = filedialog.askopenfilename(filetypes=[("Binary files", "*.bin *.dat"), ("All files", "*.*")])
        if path:
            self.payload_path.set(path)
            self._update_start_button()

    def _update_start_button(self):
        only2 = self.only_step2.get()
        has_input = bool(self.payload_path.get().strip()) if only2 else bool(self.input_path.get().strip())
        self.btn_start["state"] = "normal" if has_input else "disabled"

    def log(self, msg, color="black"):
        self.text_info.configure(state='normal')
        tag = f"tag_{color}"
        self.text_info.insert(tk.END, msg + "\n", tag)
        colors = {"red": "red", "green": "darkgreen", "orange": "#e67e22", "blue": "#0066cc"}
        self.text_info.tag_config(tag, foreground=colors.get(color, "black"))
        self.text_info.see(tk.END)
        self.text_info.configure(state='disabled')
        self.root.update_idletasks()

    def start_process(self):
        only_step2 = self.only_step2.get()

        if only_step2:
            payload_file = self.payload_path.get().strip()
            if not payload_file or not os.path.exists(payload_file):
                messagebox.showwarning("提示", "请先选择第二步输入文件")
                return
            step1_output = None
        else:
            input_file = self.input_path.get().strip()
            if not input_file or not os.path.exists(input_file):
                messagebox.showwarning("提示", "请先选择第一步输入文件")
                return

            output_file = self.output_path.get().strip()
            if not output_file:
                dirname = os.path.dirname(input_file)
                base, ext = os.path.splitext(os.path.basename(input_file))
                output_file = os.path.join(dirname, f"{base}_payload{ext}")
                self.output_path.set(output_file)

            if os.path.exists(output_file):
                if not messagebox.askyesno("覆盖确认", f"文件已存在，将覆盖：\n{output_file}"):
                    return

            step1_output = output_file
            payload_file = output_file

        self.btn_start["state"] = "disabled"
        self.btn_start["text"] = "处理中..."
        self.text_info.delete(1.0, tk.END)
        self.log("=== 开始处理 ===", "blue")

        try:
            # 第一步
            if not only_step2 and self.enable_step1.get():
                self.log(f"第一步：从 {os.path.basename(input_file)} 提取载荷...", "blue")
                success1 = self.extract_step1_optimized(input_file, step1_output)
                if not success1:
                    raise Exception("第一步提取失败")
            else:
                self.log("跳过第一步（仅执行第二步 或 已禁用）", "orange")

            # 第二步
            if self.enable_step2.get():
                self.log(f"第二步：处理 {os.path.basename(payload_file)} ...", "blue")
                success2 = self.extract_step2_optimized(payload_file)
                if success2:
                    self.log("第二步完成", "green")
            else:
                self.log("第二步已禁用", "orange")

            self.lbl_progress["text"] = "处理完成"
            messagebox.showinfo("完成", "所有指定步骤已处理完毕")
        except Exception as e:
            self.log(f"错误：{str(e)}", "red")
            messagebox.showerror("错误", str(e))
        finally:
            self.btn_start["state"] = "normal"
            self.btn_start["text"] = "开始处理"
            self.progress["value"] = 0

    def extract_step1_optimized(self, input_file, output_file):
        FRAME_SIZE = 4064
        PAYLOAD_SIZE = 4000
        HEADER_SIZE = 64
        BUFFER_SIZE = 8 * 1024 * 1024   # 8MB
        UPDATE_EVERY = 200

        file_size = os.path.getsize(input_file)
        total_frames = file_size // FRAME_SIZE
        self.log(f"第一步 - 文件大小：{file_size:,} 字节 | 帧数：{total_frames:,}")

        self.progress["maximum"] = total_frames
        extracted = 0

        with open(input_file, 'rb') as f:
            with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                with open(output_file, 'wb', buffering=BUFFER_SIZE) as fout:
                    for i in range(total_frames):
                        offset = i * FRAME_SIZE
                        payload = mm[offset + HEADER_SIZE : offset + HEADER_SIZE + PAYLOAD_SIZE]
                        fout.write(payload)
                        extracted += PAYLOAD_SIZE

                        if (i + 1) % UPDATE_EVERY == 0 or i == total_frames - 1:
                            self.progress["value"] = i + 1
                            self.lbl_progress["text"] = f"第一步：{i+1:,}/{total_frames:,} 帧 | 已提取 {extracted:,} 字节"
                            self.root.update_idletasks()

        self.log(f"第一步完成 → {os.path.basename(output_file)}  ({extracted:,} 字节)", "green")
        return extracted > 0

    def extract_step2_optimized(self, payload_file):
        FRAME_SIZE = 6448
        BUFFER_SIZE = 8 * 1024 * 1024
        UPDATE_EVERY = 200

        HEADER_MAP = {
            b'\xFA\xF3\x34\x0A\x01': '01',
            b'\xFA\xF3\x34\x0A\x02': '02',
            b'\xFA\xF3\x34\x0A\x03': '03',
            b'\xFA\xF3\x34\x0A\x04': '04',
        }

        base_name = os.path.splitext(payload_file)[0]
        out_files = {h: f"{base_name}_type_{typ}.bin" for h, typ in HEADER_MAP.items()}

        writers = {h: open(fname, 'wb', buffering=BUFFER_SIZE) for h, fname in out_files.items()}
        count = {h: 0 for h in HEADER_MAP}
        unmatched = 0
        unmatched_sample = None

        file_size = os.path.getsize(payload_file)
        total_frames = file_size // FRAME_SIZE
        self.log(f"第二步 - 文件大小：{file_size:,} 字节 | 帧数：{total_frames:,}")

        self.progress["maximum"] = total_frames
        processed = 0

        try:
            with open(payload_file, 'rb') as f:
                with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as mm:
                    for i in range(total_frames):
                        offset = i * FRAME_SIZE
                        frame = mm[offset:offset + FRAME_SIZE]
                        header = bytes(frame[:5])

                        if header in writers:
                            writers[header].write(frame)
                            count[header] += 1
                        else:
                            unmatched += 1
                            if unmatched_sample is None and frame:
                                unmatched_sample = header.hex().upper()

                        processed += 1
                        if processed % UPDATE_EVERY == 0 or processed == total_frames:
                            self.progress["value"] = processed
                            self.lbl_progress["text"] = f"第二步：{processed:,}/{total_frames:,} 帧"
                            self.root.update_idletasks()

        finally:
            for w in writers.values():
                w.close()

        self.log("第二步分离结果：", "blue")
        total_saved = 0
        for h, typ in HEADER_MAP.items():
            c = count[h]
            if c > 0:
                self.log(f"  类型 {typ}：{c:,} 帧 → {os.path.basename(out_files[h])}", "green")
                total_saved += c
            else:
                self.log(f"  类型 {typ}：0 帧", "orange")

        if unmatched > 0:
            sample_str = f"（示例：{unmatched_sample}）" if unmatched_sample else ""
            self.log(f"未匹配帧：{unmatched:,} 个 {sample_str}", "orange")

        self.log(f"第二步总计保存 {total_saved:,} 帧", "blue")
        return True


if __name__ == "__main__":
    root = tk.Tk()
    app = PayloadExtractorApp(root)
    root.mainloop()