import os
import sys

os.environ['QT_QPA_PLATFORM_PLUGIN_PATH'] = ''

from datetime import datetime, timedelta
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QLineEdit, QPushButton,
                             QGroupBox, QTextEdit, QMessageBox, QGridLayout,
                             QSpinBox, QTableWidget, QTableWidgetItem, QHeaderView)
from PyQt5.QtCore import QTimer, Qt
from PyQt5.QtGui import QFont


class WaitingTimeCalculator(QMainWindow):
    def __init__(self):
        super().__init__()
        self.init_ui()
        self.setup_timer()

    def init_ui(self):
        self.setWindowTitle('高温低温循环时间计算器')
        self.setGeometry(100, 100, 800, 700)

        # 中央窗口部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # 主布局
        main_layout = QVBoxLayout()
        central_widget.setLayout(main_layout)

        # 标题
        title_label = QLabel('高温低温循环时间计算器')
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setFont(QFont('Arial', 16, QFont.Bold))
        main_layout.addWidget(title_label)

        # 输入区域
        input_group = QGroupBox('循环参数设置')
        input_layout = QGridLayout()

        # 当前时间输入
        input_layout.addWidget(QLabel('当前时间:'), 0, 0)
        self.current_time_edit = QLineEdit()
        self.current_time_edit.setPlaceholderText('格式: YYYY-MM-DD HH:MM:SS (留空使用当前时间)')
        input_layout.addWidget(self.current_time_edit, 0, 1)

        # 使用当前时间按钮
        self.use_current_btn = QPushButton('使用当前时间')
        self.use_current_btn.clicked.connect(self.use_current_time)
        input_layout.addWidget(self.use_current_btn, 0, 2)

        # 循环次数
        input_layout.addWidget(QLabel('循环次数:'), 1, 0)
        self.cycle_count_spin = QSpinBox()
        self.cycle_count_spin.setRange(1, 100)
        self.cycle_count_spin.setValue(1)
        self.cycle_count_spin.setSuffix(' 次')
        input_layout.addWidget(self.cycle_count_spin, 1, 1)

        # 高温持续时间
        input_layout.addWidget(QLabel('高温持续时间(分钟):'), 2, 0)
        self.high_temp_edit = QLineEdit()
        self.high_temp_edit.setPlaceholderText('请输入分钟数')
        self.high_temp_edit.setText('30')
        input_layout.addWidget(self.high_temp_edit, 2, 1)

        # 低温持续时间
        input_layout.addWidget(QLabel('低温持续时间(分钟):'), 3, 0)
        self.low_temp_edit = QLineEdit()
        self.low_temp_edit.setPlaceholderText('请输入分钟数')
        self.low_temp_edit.setText('60')
        input_layout.addWidget(self.low_temp_edit, 3, 1)

        # 计算按钮
        self.calculate_btn = QPushButton('计算循环时间')
        self.calculate_btn.clicked.connect(self.calculate_cycles)
        self.calculate_btn.setStyleSheet(
            "QPushButton { background-color: #4CAF50; color: white; font-weight: bold; padding: 8px; }")
        input_layout.addWidget(self.calculate_btn, 4, 0, 1, 3)

        input_group.setLayout(input_layout)
        main_layout.addWidget(input_group)

        # 结果显示表格
        result_group = QGroupBox('循环时间表')
        result_layout = QVBoxLayout()

        self.result_table = QTableWidget()
        self.result_table.setColumnCount(5)
        self.result_table.setHorizontalHeaderLabels(['循环', '阶段', '开始时间', '结束时间', '持续时间(分钟)'])
        self.result_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        result_layout.addWidget(self.result_table)

        result_group.setLayout(result_layout)
        main_layout.addWidget(result_group)

        # 总结信息
        summary_group = QGroupBox('总结信息')
        summary_layout = QVBoxLayout()

        self.summary_text = QTextEdit()
        self.summary_text.setReadOnly(True)
        self.summary_text.setFont(QFont('Consolas', 10))
        summary_layout.addWidget(self.summary_text)

        summary_group.setLayout(summary_layout)
        main_layout.addWidget(summary_group)

        # 按钮区域
        button_layout = QHBoxLayout()

        clear_btn = QPushButton('清空所有')
        clear_btn.clicked.connect(self.clear_all)
        clear_btn.setStyleSheet("QPushButton { background-color: #f44336; color: white; padding: 8px; }")
        button_layout.addWidget(clear_btn)

        export_btn = QPushButton('导出结果')
        export_btn.clicked.connect(self.export_results)
        export_btn.setStyleSheet("QPushButton { background-color: #2196F3; color: white; padding: 8px; }")
        button_layout.addWidget(export_btn)

        main_layout.addLayout(button_layout)

    def setup_timer(self):
        self.timer = QTimer()
        self.timer.timeout.connect(self.update_display)
        self.timer.start(1000)

        self.cycle_data = []

    def use_current_time(self):
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.current_time_edit.setText(current_time)

    def calculate_cycles(self):
        try:
            # 获取当前时间
            current_time_str = self.current_time_edit.text().strip()
            if current_time_str:
                base_time = datetime.strptime(current_time_str, '%Y-%m-%d %H:%M:%S')
            else:
                base_time = datetime.now()
                self.current_time_edit.setText(base_time.strftime('%Y-%m-%d %H:%M:%S'))

            # 获取参数
            cycle_count = self.cycle_count_spin.value()
            high_temp_duration = int(self.high_temp_edit.text())
            low_temp_duration = int(self.low_temp_edit.text())

            # 计算循环时间
            self.cycle_data = []
            current_start_time = base_time

            for cycle in range(1, cycle_count + 1):
                # 高温阶段
                high_temp_end = current_start_time + timedelta(minutes=high_temp_duration)
                self.cycle_data.append({
                    'cycle': cycle,
                    'phase': '高温',
                    'start_time': current_start_time,
                    'end_time': high_temp_end,
                    'duration': high_temp_duration
                })

                # 低温阶段
                low_temp_end = high_temp_end + timedelta(minutes=low_temp_duration)
                self.cycle_data.append({
                    'cycle': cycle,
                    'phase': '低温',
                    'start_time': high_temp_end,
                    'end_time': low_temp_end,
                    'duration': low_temp_duration
                })

                current_start_time = low_temp_end

            # 更新显示
            self.update_display()

        except ValueError as e:
            QMessageBox.warning(self, '输入错误',
                                '请检查输入格式是否正确：\n- 时间格式: YYYY-MM-DD HH:MM:SS\n- 持续时间: 数字')
        except Exception as e:
            QMessageBox.critical(self, '错误', f'发生未知错误: {str(e)}')

    def update_display(self):
        # 更新表格
        self.result_table.setRowCount(len(self.cycle_data))

        for row, data in enumerate(self.cycle_data):
            self.result_table.setItem(row, 0, QTableWidgetItem(str(data['cycle'])))
            self.result_table.setItem(row, 1, QTableWidgetItem(data['phase']))
            self.result_table.setItem(row, 2, QTableWidgetItem(data['start_time'].strftime('%Y-%m-%d %H:%M:%S')))
            self.result_table.setItem(row, 3, QTableWidgetItem(data['end_time'].strftime('%Y-%m-%d %H:%M:%S')))
            self.result_table.setItem(row, 4, QTableWidgetItem(str(data['duration'])))

        # 更新总结信息
        if self.cycle_data:
            total_cycles = self.cycle_count_spin.value()
            total_time = self.cycle_data[-1]['end_time'] - self.cycle_data[0]['start_time']
            high_temp_total = sum(d['duration'] for d in self.cycle_data if d['phase'] == '高温')
            low_temp_total = sum(d['duration'] for d in self.cycle_data if d['phase'] == '低温')

            summary = f"""总结信息:
================================
总循环次数: {total_cycles} 次
总高温时间: {high_temp_total} 分钟
总低温时间: {low_temp_total} 分钟
总运行时间: {self.format_timedelta(total_time)}
开始时间: {self.cycle_data[0]['start_time'].strftime('%Y-%m-%d %H:%M:%S')}
结束时间: {self.cycle_data[-1]['end_time'].strftime('%Y-%m-%d %H:%M:%S')}
================================

当前状态: {self.get_current_status()}"""

            self.summary_text.setText(summary)

    def get_current_status(self):
        if not self.cycle_data:
            return "未开始计算"

        current_time = datetime.now()

        for data in self.cycle_data:
            if data['start_time'] <= current_time <= data['end_time']:
                remaining = data['end_time'] - current_time
                return f"当前处于第{data['cycle']}循环 {data['phase']}阶段，剩余时间: {self.format_timedelta(remaining)}"
            elif current_time < data['start_time']:
                waiting = data['start_time'] - current_time
                return f"等待第{data['cycle']}循环 {data['phase']}阶段开始，剩余: {self.format_timedelta(waiting)}"

        return "所有循环已完成"

    def format_timedelta(self, td):
        """格式化时间差为易读格式"""
        total_seconds = int(td.total_seconds())
        if total_seconds < 0:
            return "已结束"

        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        days, hours = divmod(hours, 24)

        if days > 0:
            return f"{days}天 {hours:02d}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def export_results(self):
        if not self.cycle_data:
            QMessageBox.warning(self, '警告', '没有数据可导出')
            return

        try:
            filename = f"循环时间表_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
            with open(filename, 'w', encoding='utf-8') as f:
                f.write("高温低温循环时间表\n")
                f.write("=" * 50 + "\n")
                f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"循环次数: {self.cycle_count_spin.value()}\n")
                f.write(f"高温持续时间: {self.high_temp_edit.text()} 分钟\n")
                f.write(f"低温持续时间: {self.low_temp_edit.text()} 分钟\n")
                f.write("=" * 50 + "\n\n")

                f.write("循环时间表:\n")
                f.write("循环\t阶段\t开始时间\t\t结束时间\t\t持续时间(分钟)\n")
                f.write("-" * 80 + "\n")

                for data in self.cycle_data:
                    f.write(f"{data['cycle']}\t{data['phase']}\t"
                            f"{data['start_time'].strftime('%Y-%m-%d %H:%M:%S')}\t"
                            f"{data['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\t"
                            f"{data['duration']}\n")

                # 写入总结信息
                total_time = self.cycle_data[-1]['end_time'] - self.cycle_data[0]['start_time']
                high_temp_total = sum(d['duration'] for d in self.cycle_data if d['phase'] == '高温')
                low_temp_total = sum(d['duration'] for d in self.cycle_data if d['phase'] == '低温')

                f.write("\n总结信息:\n")
                f.write(f"总高温时间: {high_temp_total} 分钟\n")
                f.write(f"总低温时间: {low_temp_total} 分钟\n")
                f.write(f"总运行时间: {self.format_timedelta(total_time)}\n")
                f.write(f"开始时间: {self.cycle_data[0]['start_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"结束时间: {self.cycle_data[-1]['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")

            QMessageBox.information(self, '导出成功', f'结果已导出到: {filename}')

        except Exception as e:
            QMessageBox.critical(self, '导出失败', f'导出时发生错误: {str(e)}')

    def clear_all(self):
        self.current_time_edit.clear()
        self.cycle_count_spin.setValue(1)
        self.high_temp_edit.setText('30')
        self.low_temp_edit.setText('60')
        self.result_table.setRowCount(0)
        self.summary_text.clear()
        self.cycle_data = []


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    calculator = WaitingTimeCalculator()
    calculator.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()