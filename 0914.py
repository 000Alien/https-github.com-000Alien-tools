import sys
import os
import shutil
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QPushButton, QLabel, QTextEdit,
                             QFileDialog, QSpinBox, QMessageBox, QProgressBar,
                             QTabWidget, QLineEdit, QGroupBox)
from PyQt5.QtCore import Qt


class FileSplitterApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.initUI()
        self.df = None
        self.file_path = None

    def initUI(self):
        self.setWindowTitle('文件与CSV分割工具')
        self.setGeometry(100, 100, 800, 600)

        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        layout = QVBoxLayout()
        central_widget.setLayout(layout)

        # 创建选项卡
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)

        # 文件分割选项卡
        self.file_tab = QWidget()
        self.setup_file_tab()
        self.tabs.addTab(self.file_tab, "文件分割")

        # CSV分割选项卡
        self.csv_tab = QWidget()
        self.setup_csv_tab()
        self.tabs.addTab(self.csv_tab, "CSV分割")

        # 状态栏
        self.statusBar().showMessage('就绪')

    def setup_file_tab(self):
        layout = QVBoxLayout()
        self.file_tab.setLayout(layout)

        # 源文件夹选择
        source_group = QGroupBox("源文件夹")
        source_layout = QHBoxLayout()
        self.source_edit = QLineEdit()
        self.source_browse_btn = QPushButton('浏览...')
        self.source_browse_btn.clicked.connect(self.browse_source)
        source_layout.addWidget(self.source_edit)
        source_layout.addWidget(self.source_browse_btn)
        source_group.setLayout(source_layout)
        layout.addWidget(source_group)

        # 目标文件夹选择
        target_group = QGroupBox("目标文件夹")
        target_layout = QHBoxLayout()
        self.target_edit = QLineEdit()
        self.target_browse_btn = QPushButton('浏览...')
        self.target_browse_btn.clicked.connect(self.browse_target)
        target_layout.addWidget(self.target_edit)
        target_layout.addWidget(self.target_browse_btn)
        target_group.setLayout(target_layout)
        layout.addWidget(target_group)

        # 文件数量设置
        count_group = QGroupBox("分割设置")
        count_layout = QHBoxLayout()
        count_layout.addWidget(QLabel('每个子文件夹文件数:'))
        self.files_per_folder = QSpinBox()
        self.files_per_folder.setRange(1, 10000)
        self.files_per_folder.setValue(1690)
        count_layout.addWidget(self.files_per_folder)
        count_layout.addStretch()
        count_group.setLayout(count_layout)
        layout.addWidget(count_group)

        # 进度条
        self.file_progress = QProgressBar()
        self.file_progress.setVisible(False)
        layout.addWidget(self.file_progress)

        # 操作按钮
        self.file_process_btn = QPushButton('开始分割')
        self.file_process_btn.clicked.connect(self.process_files)
        layout.addWidget(self.file_process_btn)

        # 日志显示
        layout.addWidget(QLabel('处理日志:'))
        self.file_log = QTextEdit()
        self.file_log.setReadOnly(True)
        layout.addWidget(self.file_log)

    def setup_csv_tab(self):
        layout = QVBoxLayout()
        self.csv_tab.setLayout(layout)

        # 文件选择部分
        file_group = QGroupBox("CSV文件选择")
        file_layout = QHBoxLayout()
        self.csv_select_btn = QPushButton('选择CSV文件')
        self.csv_select_btn.clicked.connect(self.select_csv_file)
        self.csv_file_label = QLabel('未选择文件')
        file_layout.addWidget(self.csv_select_btn)
        file_layout.addWidget(self.csv_file_label)
        file_group.setLayout(file_layout)
        layout.addWidget(file_group)

        # 行数设置部分
        rows_group = QGroupBox("分割设置")
        rows_layout = QHBoxLayout()
        rows_layout.addWidget(QLabel('每份文件行数:'))
        self.rows_spinbox = QSpinBox()
        self.rows_spinbox.setRange(1, 10000)
        self.rows_spinbox.setValue(130)
        rows_layout.addWidget(self.rows_spinbox)
        rows_layout.addStretch()
        rows_group.setLayout(rows_layout)
        layout.addWidget(rows_group)

        # 进度条
        self.csv_progress = QProgressBar()
        self.csv_progress.setVisible(False)
        layout.addWidget(self.csv_progress)

        # 操作按钮
        self.csv_process_btn = QPushButton('处理文件')
        self.csv_process_btn.clicked.connect(self.process_csv)
        self.csv_process_btn.setEnabled(False)
        layout.addWidget(self.csv_process_btn)

        # 日志显示
        layout.addWidget(QLabel('处理日志:'))
        self.csv_log = QTextEdit()
        self.csv_log.setReadOnly(True)
        layout.addWidget(self.csv_log)

    def browse_source(self):
        folder = QFileDialog.getExistingDirectory(self, '选择源文件夹')
        if folder:
            self.source_edit.setText(folder)
            self.file_log.append(f'已选择源文件夹: {folder}')

    def browse_target(self):
        folder = QFileDialog.getExistingDirectory(self, '选择目标文件夹')
        if folder:
            self.target_edit.setText(folder)
            self.file_log.append(f'已选择目标文件夹: {folder}')

    def select_csv_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, '选择CSV文件', '', 'CSV文件 (*.csv);;所有文件 (*)')

        if file_path:
            self.file_path = file_path
            self.csv_file_label.setText(os.path.basename(file_path))
            self.csv_log.append(f'已选择文件: {file_path}')

            try:
                # 尝试读取文件以验证格式，不将第一行作为标题
                self.df = pd.read_csv(file_path, header=None)
                self.csv_process_btn.setEnabled(True)
                self.csv_log.append(f'文件读取成功，总行数: {len(self.df)}')
                self.statusBar().showMessage(f'文件读取成功，总行数: {len(self.df)}')
            except Exception as e:
                self.csv_log.append(f'文件读取错误: {str(e)}')
                QMessageBox.critical(self, '错误', f'无法读取CSV文件: {str(e)}')
                self.csv_process_btn.setEnabled(False)

    def process_files(self):
        source = self.source_edit.text()
        target = self.target_edit.text()

        if not source or not target:
            QMessageBox.warning(self, "警告", "请先选择源文件夹和目标文件夹")
            return

        files_per_folder = self.files_per_folder.value()
        if files_per_folder <= 0:
            QMessageBox.warning(self, "警告", "请输入有效的文件数量")
            return

        # 获取源文件夹中的所有文件
        try:
            all_files = [f for f in os.listdir(source) if os.path.isfile(os.path.join(source, f))]
            all_files.sort()  # 按文件名排序
        except Exception as e:
            QMessageBox.critical(self, "错误", f"无法读取源文件夹: {str(e)}")
            return

        total_files = len(all_files)
        if total_files == 0:
            QMessageBox.warning(self, "警告", "源文件夹中没有文件")
            return

        # 计算需要的子文件夹数量
        num_folders = (total_files + files_per_folder - 1) // files_per_folder

        self.file_log.append(f"开始处理: 共{total_files}个文件，将分割到{num_folders}个子文件夹中")
        self.file_progress.setMaximum(total_files)
        self.file_progress.setValue(0)
        self.file_progress.setVisible(True)

        # 创建子文件夹并移动文件
        files_copied = 0
        for folder_num in range(1, num_folders + 1):
            folder_name = str(folder_num)
            folder_path = os.path.join(target, folder_name)

            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                self.file_log.append(f"创建子文件夹: {folder_name}")

            # 计算当前文件夹应该包含的文件范围
            start_index = (folder_num - 1) * files_per_folder
            end_index = min(folder_num * files_per_folder, total_files)

            for i in range(start_index, end_index):
                src_file = os.path.join(source, all_files[i])
                dst_file = os.path.join(folder_path, all_files[i])

                try:
                    shutil.copy2(src_file, dst_file)  # 使用copy2保留元数据
                    files_copied += 1
                    self.file_progress.setValue(files_copied)

                    if files_copied % 100 == 0:
                        self.file_log.append(f"已处理 {files_copied}/{total_files} 个文件")
                        QApplication.processEvents()  # 更新UI

                except Exception as e:
                    self.file_log.append(f"错误: 无法复制文件 {all_files[i]}: {str(e)}")

        # 验证结果
        self.file_log.append("处理完成，正在验证结果...")
        total_copied = 0
        for folder_num in range(1, num_folders + 1):
            folder_name = str(folder_num)
            folder_path = os.path.join(target, folder_name)

            if os.path.exists(folder_path):
                files_in_folder = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
                total_copied += len(files_in_folder)
                self.file_log.append(f"子文件夹 {folder_name} 中有 {len(files_in_folder)} 个文件")

        if total_copied == total_files:
            self.file_log.append(f"验证成功! 总共复制了 {total_copied} 个文件")
            QMessageBox.information(self, "完成", f"文件分割完成! 总共处理了 {total_copied} 个文件")
        else:
            self.file_log.append(
                f"警告: 复制文件数量不匹配。源文件夹有 {total_files} 个文件，但只复制了 {total_copied} 个文件")
            QMessageBox.warning(self, "警告",
                                f"复制文件数量不匹配。源文件夹有 {total_files} 个文件，但只复制了 {total_copied} 个文件")

        self.file_progress.setVisible(False)

    def process_csv(self):
        if self.df is None or self.file_path is None:
            return

        # 选择保存目录
        save_dir = QFileDialog.getExistingDirectory(self, '选择保存目录')
        if not save_dir:
            return

        rows_per_file = self.rows_spinbox.value()
        total_rows = len(self.df)
        num_files = (total_rows + rows_per_file - 1) // rows_per_file

        self.csv_log.append(f'开始处理，将分割为 {num_files} 个文件')
        self.csv_progress.setVisible(True)
        self.csv_progress.setMaximum(num_files)

        success_count = 0
        error_count = 0

        for i in range(num_files):
            start_idx = i * rows_per_file
            end_idx = min((i + 1) * rows_per_file, total_rows)

            # 确保不会超出数据范围
            if start_idx >= total_rows:
                break

            # 获取数据块
            chunk = self.df.iloc[start_idx:end_idx]

            # 保存分块文件
            file_name = f"{i + 1}.csv"
            file_path = os.path.join(save_dir, file_name)

            try:
                # 保存时不包含标题行
                chunk.to_csv(file_path, index=False, header=False)
                actual_rows = len(chunk)
                self.csv_log.append(f'已保存: {file_path} (行 {start_idx + 1}-{end_idx}, 实际行数: {actual_rows})')

                # 校验文件 - 只核对行数是否一致
                expected_rows = rows_per_file if i < num_files - 1 else (total_rows % rows_per_file or rows_per_file)

                if actual_rows == expected_rows:
                    self.csv_log.append(
                        f'✓ 文件 {file_name} 行数校验成功 (预期: {expected_rows}, 实际: {actual_rows})')
                    success_count += 1
                else:
                    self.csv_log.append(
                        f'✗ 文件 {file_name} 行数校验失败 (预期: {expected_rows}, 实际: {actual_rows})')
                    error_count += 1

            except Exception as e:
                self.csv_log.append(f'保存文件 {file_name} 时出错: {str(e)}')
                error_count += 1

            self.csv_progress.setValue(i + 1)
            QApplication.processEvents()  # 更新UI

        # 处理完成
        self.csv_progress.setVisible(False)
        self.csv_log.append(f'处理完成! 成功: {success_count}, 失败: {error_count}')

        if error_count == 0:
            QMessageBox.information(self, '完成', '所有文件已成功处理并校验!')
        else:
            QMessageBox.warning(self, '完成',
                                f'处理完成，但有 {error_count} 个文件出现错误。请查看日志获取详细信息。')


def main():
    app = QApplication(sys.argv)
    window = FileSplitterApp()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()