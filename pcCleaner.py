import csv
import os
import platform
import subprocess
import sys
import time
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

from PySide6.QtCore import QAbstractTableModel, QSortFilterProxyModel, Qt, QThreadPool, QRunnable, Signal, QObject, Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QTableView,
    QVBoxLayout,
    QWidget,
    QComboBox,
    QAbstractItemView,
)


@dataclass
class RowData:
    name: str
    path: str
    size: int
    files: int
    percent: float = 0.0


class ScanSignals(QObject):
    placeholders_ready = Signal(list)
    row_ready = Signal(str, str, object, object)  # name, path, size, files
    progress = Signal(object, object)  # done, total
    finished = Signal(float)  # duration
    error = Signal(str)


class DirectoryScanTask(QRunnable):
    def __init__(self, root_path: str, cancelled):
        super().__init__()
        self.root_path = root_path
        self.cancelled = cancelled
        self.signals = ScanSignals()

    def run(self):
        start = time.time()
        try:
            subdirs = []
            with os.scandir(self.root_path) as it:
                for entry in it:
                    if self.cancelled():
                        self.signals.finished.emit(time.time() - start)
                        return
                    if entry.is_dir(follow_symlinks=False):
                        subdirs.append((entry.name, os.path.normpath(entry.path)))

            # Emit placeholders immediately from the background so the UI doesn't freeze discovery
            placeholders = [RowData(name=n, path=p, size=-1, files=-1) for n, p in subdirs]
            self.signals.placeholders_ready.emit(placeholders)

            if not subdirs:
                self.signals.progress.emit(0, 0)
                self.signals.finished.emit(time.time() - start)
                return

            # Alphabetical sort for UI stability during fresh scan
            subdirs.sort(key=lambda item: item[0].lower())

            self.signals.progress.emit(0, len(subdirs))
            done = 0

            max_workers = min(16, max(6, (os.cpu_count() or 6)))
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(self._get_folder_info, path): (name, path) for name, path in subdirs
                }
                for future in as_completed(future_map):
                    if self.cancelled():
                        executor.shutdown(wait=False, cancel_futures=True)
                        self.signals.finished.emit(time.time() - start)
                        return
                    name, path = future_map[future]
                    try:
                        size, files = future.result()
                    except Exception:
                        size, files = 0, 0
                    self.signals.row_ready.emit(name, path, size, files)
                    done += 1
                    self.signals.progress.emit(done, len(subdirs))
            self.signals.finished.emit(time.time() - start)
        except Exception as exc:
            self.signals.error.emit(str(exc))
            self.signals.finished.emit(time.time() - start)

    def _get_folder_info(self, directory: str):
        total_size = 0
        total_files = 0
        stack = [directory]
        while stack and not self.cancelled():
            current = stack.pop()
            try:
                with os.scandir(current) as it:
                    for item in it:
                        if self.cancelled():
                            return 0, 0
                        try:
                            if item.is_file(follow_symlinks=False):
                                total_size += item.stat(follow_symlinks=False).st_size
                                total_files += 1
                            elif item.is_dir(follow_symlinks=False):
                                stack.append(item.path)
                        except (PermissionError, FileNotFoundError, OSError):
                            continue
            except (PermissionError, FileNotFoundError, OSError):
                continue
        return total_size, total_files


class TableModel(QAbstractTableModel):
    headers = ["Directory", "Size", "Files", "% of Parent"]

    def __init__(self):
        super().__init__()
        self.rows: list[RowData] = []
        self._path_to_idx = {}

    def rowCount(self, parent=None):
        return len(self.rows)

    def columnCount(self, parent=None):
        return len(self.headers)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self.rows[index.row()]
        col = index.column()
        if role == Qt.DisplayRole:
            if col == 0:
                return row.name
            if col == 1:
                if row.size < 0:
                    return "Calculating..."
                return self._format_size(row.size)
            if col == 2:
                if row.files < 0:
                    return "..."
                return f"{row.files:,}"
            if col == 3:
                if row.size < 0:
                    return "--"
                return f"{row.percent:.1f}%"
        if role == Qt.UserRole:
            if col == 0:
                return row.name.lower()
            if col == 1:
                return row.size
            if col == 2:
                return row.files
            if col == 3:
                return row.percent
        if role == Qt.TextAlignmentRole and col in (1, 2, 3):
            return int(Qt.AlignRight | Qt.AlignVCenter)
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self.headers[section]
        return super().headerData(section, orientation, role)

    def set_rows(self, rows: list[RowData]):
        self.beginResetModel()
        self.rows = rows
        self._path_to_idx = {r.path: i for i, r in enumerate(rows)}
        self.endResetModel()

    def add_row(self, row: RowData):
        pos = len(self.rows)
        self.beginInsertRows(self.createIndex(-1, -1), pos, pos)
        self.rows.append(row)
        self._path_to_idx[row.path] = pos
        self.endInsertRows()

    def upsert_row(self, row: RowData):
        if row.path in self._path_to_idx:
            i = self._path_to_idx[row.path]
            self.rows[i] = row
            left = self.index(i, 0)
            right = self.index(i, self.columnCount() - 1)
            self.dataChanged.emit(left, right, [Qt.DisplayRole, Qt.UserRole])
            return
        self.add_row(row)

    def update_percents(self):
        real_rows = [r for r in self.rows if r.size >= 0]
        total = sum(r.size for r in real_rows)
        if total <= 0:
            for r in real_rows:
                r.percent = 0.0
        else:
            for r in real_rows:
                r.percent = (r.size / total) * 100.0
        if self.rows:
            top_left = self.index(0, 3)
            bottom_right = self.index(len(self.rows) - 1, 3)
            self.dataChanged.emit(top_left, bottom_right, [Qt.DisplayRole, Qt.UserRole])

    def _format_size(self, size_bytes: int):
        if size_bytes < 1024:
            return f"{size_bytes} B"
        if size_bytes < 1024**2:
            return f"{size_bytes / 1024:.2f} KB"
        if size_bytes < 1024**3:
            return f"{size_bytes / 1024**2:.2f} MB"
        if size_bytes < 1024**4:
            return f"{size_bytes / 1024**3:.2f} GB"
        return f"{size_bytes / 1024**4:.2f} TB"


class SortProxy(QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self.min_size_bytes = 0

    def lessThan(self, left, right):
        left_val = self.sourceModel().data(left, Qt.UserRole)
        right_val = self.sourceModel().data(right, Qt.UserRole)
        return left_val < right_val

    def set_min_size_bytes(self, value: int):
        self.min_size_bytes = max(0, int(value))
        self.invalidate()

    def filterAcceptsRow(self, source_row, source_parent):
        model = self.sourceModel()
        name_idx = model.index(source_row, 0, source_parent)
        size_idx = model.index(source_row, 1, source_parent)

        name_val = model.data(name_idx, Qt.DisplayRole) or ""
        size_val = model.data(size_idx, Qt.UserRole) or 0

        search_text = self.filterRegularExpression().pattern()
        name_ok = True if not search_text else (search_text.lower() in str(name_val).lower())
        # Keep placeholder rows (size < 0 => "Calculating...") visible during scan.
        size_ok = int(size_val) < 0 or int(size_val) >= self.min_size_bytes
        return name_ok and size_ok


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Cleaner - PySide6")
        self.resize(1080, 680)
        
        if os.path.exists("E:\\pcCleaner\\broom.ico"):
            self.setWindowIcon(QIcon("E:\\pcCleaner\\broom.ico"))

        self.last_path = ""
        self.cancel_scan_flag = False
        self.scan_total = 0
        self.scan_done = 0
        self.active_scan_id = 0
        self.thread_pool = QThreadPool.globalInstance()

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        top = QHBoxLayout()
        root.addLayout(top)

        self.drive_combo = QComboBox()
        self.drive_combo.setMinimumWidth(100)
        top.addWidget(self.drive_combo)
        self.refresh_drives()

        self.scan_drive_btn = QPushButton("Scan Drive")
        self.scan_drive_btn.clicked.connect(self.scan_selected_drive)
        top.addWidget(self.scan_drive_btn)

        self.select_btn = QPushButton("Select Directory")
        self.select_btn.clicked.connect(self.select_directory)
        top.addWidget(self.select_btn)

        self.up_btn = QPushButton("Up")
        self.up_btn.clicked.connect(self.navigate_up)
        self.up_btn.setEnabled(False)
        top.addWidget(self.up_btn)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.cancel_scan)
        self.cancel_btn.setEnabled(False)
        top.addWidget(self.cancel_btn)

        self.refresh_btn = QPushButton("Refresh")
        self.refresh_btn.clicked.connect(self.refresh_scan)
        top.addWidget(self.refresh_btn)

        self.export_btn = QPushButton("Export CSV")
        self.export_btn.clicked.connect(self.export_to_csv)
        top.addWidget(self.export_btn)

        self.open_btn = QPushButton("Open in Explorer")
        self.open_btn.clicked.connect(self.open_in_explorer)
        top.addWidget(self.open_btn)

        top.addWidget(QLabel("Search:"))
        self.search_edit = QLineEdit()
        self.search_edit.textChanged.connect(self._apply_search)
        self.search_edit.setPlaceholderText("Filter by name")
        top.addWidget(self.search_edit)

        top.addWidget(QLabel("Min MB:"))
        self.min_size_edit = QLineEdit()
        self.min_size_edit.setMaximumWidth(90)
        self.min_size_edit.setPlaceholderText("0")
        top.addWidget(self.min_size_edit)

        self.filter_btn = QPushButton("Apply Filter")
        self.filter_btn.clicked.connect(self.apply_filter)
        top.addWidget(self.filter_btn)

        self.clear_filter_btn = QPushButton("Clear Filters")
        self.clear_filter_btn.clicked.connect(self.clear_filters)
        top.addWidget(self.clear_filter_btn)

        path_layout = QHBoxLayout()
        root.addLayout(path_layout)
        path_layout.addWidget(QLabel("Current Path:"))
        self.current_path_edit = QLineEdit()
        self.current_path_edit.setReadOnly(True)
        path_layout.addWidget(self.current_path_edit)
        self.copy_path_btn = QPushButton("Copy")
        self.copy_path_btn.clicked.connect(self.copy_path_to_clipboard)
        path_layout.addWidget(self.copy_path_btn)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setFormat("%v / %m folders")
        self.progress.setVisible(False)
        root.addWidget(self.progress)

        self.model = TableModel()
        self.proxy = SortProxy()
        self.proxy.setSourceModel(self.model)
        self.proxy.setFilterKeyColumn(0)
        self.proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)

        self.table = QTableView()
        self.table.setModel(self.proxy)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SingleSelection)
        self.table.setSortingEnabled(True)
        self.table.sortByColumn(1, Qt.DescendingOrder)
        self.table.doubleClicked.connect(self.navigate_into_directory)
        self.table.horizontalHeader().setStretchLastSection(True)
        root.addWidget(self.table)

        self.status = QLabel("Select a directory to begin.")
        root.addWidget(self.status)

    def refresh_drives(self):
        self.drive_combo.clear()
        if platform.system() == "Windows":
            drives = [f"{chr(c)}:\\" for c in range(ord("A"), ord("Z") + 1) if os.path.exists(f"{chr(c)}:\\")]
        else:
            drives = ["/"]
        self.drive_combo.addItems(drives)

    def set_scanning_ui(self, scanning: bool):
        self.cancel_btn.setEnabled(scanning)
        # Allow switching directories/drives at any time.
        self.select_btn.setEnabled(True)
        self.scan_drive_btn.setEnabled(True)
        self._update_up_button_state()
        # Keep progress bar hidden; row placeholders indicate loading.
        self.progress.setVisible(False)
        self.table.setSortingEnabled(not scanning)
        if not scanning:
            self.progress.setRange(0, 100)
            self.progress.setValue(0)
            self.progress.setFormat("%v / %m folders")

    def select_directory(self):
        path = QFileDialog.getExistingDirectory(self, "Select Directory")
        if path:
            self.start_scan(path)

    def scan_selected_drive(self):
        path = self.drive_combo.currentText().strip()
        if path:
            self.start_scan(path)

    def start_scan(self, path: str):
        path = os.path.normpath(path)
        self.current_path_edit.setText(path)
        self.cancel_scan_flag = True # Signal current scan to stop
        self.active_scan_id += 1
        scan_id = self.active_scan_id
        self.cancel_scan_flag = False

        self.scan_total = 0
        self.scan_done = 0
        self.last_path = path
        self._reset_filters_for_new_scan()

        # Clear table and update UI state immediately
        self.model.set_rows([])
        self.set_scanning_ui(True)
        self.status.setText(f"Scanning: {path} ...")

        task = DirectoryScanTask(
            path,
            cancelled=lambda sid=scan_id: self.cancel_scan_flag or sid != self.active_scan_id,
        )
        
        def handle_placeholders(rows, sid=scan_id):
            if int(sid) == self.active_scan_id:
                self.model.set_rows(rows)
                self._select_first_visible_row()

        task.signals.placeholders_ready.connect(handle_placeholders)
        task.signals.row_ready.connect(lambda name, sub_path, size, files, sid=scan_id: self._on_row_ready(sid, name, sub_path, size, files))
        task.signals.progress.connect(lambda done, total, sid=scan_id: self._on_scan_progress(sid, done, total))
        task.signals.error.connect(lambda message, sid=scan_id: self._on_scan_error(sid, message))
        task.signals.finished.connect(lambda duration, sid=scan_id: self._on_scan_finished(sid, path, duration))
        self.thread_pool.start(task)

    @Slot(object, str, str, object, object)
    def _on_row_ready(self, scan_id: object, name: str, sub_path: str, size: object, files: object):
        if int(scan_id) != self.active_scan_id:
            return
        self.model.upsert_row(RowData(name=name, path=sub_path, size=int(size), files=int(files)))

    @Slot(object, str)
    def _on_scan_error(self, scan_id: object, message: str):
        if int(scan_id) != self.active_scan_id:
            return
        self.status.setText(f"Scan error: {message}")

    @Slot(object, object, object)
    def _on_scan_progress(self, scan_id: object, done: object, total: object):
        if int(scan_id) != self.active_scan_id:
            return
        self.scan_done = int(done)
        self.scan_total = int(total)
        if self.scan_total <= 0:
            self.status.setText(f"Scanning: {self.last_path} ... loading folders")
            return

        # Throttle percentage updates to prevent UI lag (O(N^2) complexity on row updates)
        # Update every 50 folders, or if the scan is small, or if it's finished.
        is_done = self.scan_done >= self.scan_total
        if self.scan_done > 0 and (self.scan_done % 50 == 0 or self.scan_total < 50 or is_done):
            self.model.update_percents()
        self.status.setText(f"Scanning: {self.last_path} ... loading folders ({self.scan_done}/{self.scan_total})")

    def _on_scan_finished(self, scan_id: object, path: str, duration: float):
        if int(scan_id) != self.active_scan_id:
            return
        scan_completed = (not self.cancel_scan_flag) and (self.scan_total == 0 or self.scan_done >= self.scan_total)
        self.model.update_percents()

        total_size = sum(r.size for r in self.model.rows)
        total_files = sum(r.files for r in self.model.rows)

        if self.cancel_scan_flag:
            self.status.setText("Scan cancelled.")
        else:
            if self.model.rowCount() == 0:
                self.status.setText(f"Scan complete in {duration:.2f}s. No subfolders found in {path}.")
            elif self.proxy.rowCount() == 0:
                self.status.setText(
                    f"Scan complete in {duration:.2f}s. Results hidden by active filters. Click Clear Filters."
                )
            else:
                self.status.setText(
                    f"Scan complete in {duration:.2f}s. Total: {self.model._format_size(total_size)}, {total_files:,} files. Current: {path}"
                )
        self.set_scanning_ui(False)
        self._select_first_visible_row()

    def cancel_scan(self):
        self.cancel_scan_flag = True
        self.status.setText("Cancelling scan...")

    def refresh_scan(self):
        if not self.last_path:
            self.status.setText("No directory selected.")
            return
        self.start_scan(self.last_path)

    def navigate_up(self):
        if not self.last_path:
            self.status.setText("No directory selected.")
            return
        parent = os.path.normpath(self._get_parent_path(self.last_path))
        if not parent:
            self.status.setText("Cannot navigate up: already at root.")
            return
        self.start_scan(parent)

    def navigate_into_directory(self):
        idx = self.table.currentIndex()
        if not idx.isValid():
            return
        src_idx = self.proxy.mapToSource(idx)
        row = self.model.rows[src_idx.row()]
        if os.path.isdir(row.path):
            self.start_scan(row.path)

    def copy_path_to_clipboard(self):
        path = self.current_path_edit.text()
        if path:
            QApplication.clipboard().setText(path)
            self.status.setText(f"Path copied to clipboard.")

    def apply_filter(self):
        text = self.min_size_edit.text().strip()
        try:
            min_mb = float(text) if text else 0.0
        except ValueError:
            min_mb = 0.0
        threshold = int(min_mb * 1024 * 1024)
        self.proxy.set_min_size_bytes(threshold)
        self.status.setText(f"Filter applied: minimum {min_mb:.2f} MB")
        self._select_first_visible_row()

    def _apply_search(self, text: str):
        self.proxy.setFilterFixedString(text)
        self._select_first_visible_row()

    def clear_filters(self):
        self.search_edit.clear()
        self.min_size_edit.clear()
        self.proxy.set_min_size_bytes(0)
        self.proxy.setFilterFixedString("")
        self.status.setText("Filters cleared.")
        self._select_first_visible_row()

    def _reset_filters_for_new_scan(self):
        """Ensure a new scan starts with visible rows."""
        if self.search_edit.text():
            self.search_edit.clear()
        if self.min_size_edit.text():
            self.min_size_edit.clear()
        self.proxy.set_min_size_bytes(0)
        self.proxy.setFilterFixedString("")

    def _select_first_visible_row(self):
        if self.proxy.rowCount() <= 0:
            self.table.clearSelection()
            return
        idx = self.proxy.index(0, 0)
        self.table.selectRow(0)
        self.table.setCurrentIndex(idx)

    def export_to_csv(self):
        if not self.model.rows:
            self.status.setText("No data to export.")
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", "", "CSV Files (*.csv)")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["Directory", "Path", "SizeBytes", "Files", "Percent"])
                for r in self.model.rows:
                    writer.writerow([r.name, r.path, r.size, r.files, f"{r.percent:.2f}"])
            self.status.setText(f"Exported to {path}")
        except Exception as exc:
            self.status.setText(f"Export failed: {exc}")

    def open_in_explorer(self):
        idx = self.table.currentIndex()
        full_path = ""
        if idx.isValid():
            src_idx = self.proxy.mapToSource(idx)
            if src_idx.isValid():
                row = self.model.rows[src_idx.row()]
                full_path = row.path
        if not full_path:
            full_path = self.last_path
        if not full_path:
            self.status.setText("No directory selected.")
            return
        try:
            if platform.system() == "Windows":
                os.startfile(full_path)  # type: ignore[attr-defined]
            elif platform.system() == "Linux":
                subprocess.run(["xdg-open", full_path], check=True)
            elif platform.system() == "Darwin":
                subprocess.run(["open", full_path], check=True)
            else:
                raise RuntimeError("Unsupported platform.")
            self.status.setText(f"Opened {full_path}")
        except Exception as exc:
            self.status.setText(f"Open failed: {exc}")

    def _get_parent_path(self, path: str):
        normalized = os.path.normpath(path)
        parent = os.path.dirname(normalized)
        if not parent or parent == normalized:
            return ""
        return parent

    def _update_up_button_state(self):
        self.up_btn.setEnabled(bool(self._get_parent_path(self.last_path)))

    def closeEvent(self, event):
        self.cancel_scan_flag = True
        self.thread_pool.waitForDone(2000)
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()