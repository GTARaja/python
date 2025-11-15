import sys
from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QSplashScreen
)
from PyQt5.QtSvg import QSvgWidget
from PyQt5.QtCore import Qt, QTimer


class SvgSplash(QWidget):
    def __init__(self, svg_file, width=300, height=300):
        super().__init__()

        self.setWindowFlags(
            Qt.FramelessWindowHint |
            Qt.WindowStaysOnTopHint |
            Qt.SplashScreen
        )

        self.setAttribute(Qt.WA_TranslucentBackground)

        layout = QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        self.svg = QSvgWidget(svg_file)
        self.svg.setFixedSize(width, height)

        layout.addWidget(self.svg, alignment=Qt.AlignCenter)
        self.setLayout(layout)


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Main Window")
        self.resize(800, 600)


def main():
    app = QApplication(sys.argv)

    # 🔵 REPLACE THIS WITH YOUR FINAL ANIMATED SVG FILE
    svg_path = "qtest_animated.svg"

    splash = SvgSplash(svg_path, width=350, height=350)
    splash.show()

    # Ensures UI updates immediately
    app.processEvents()

    def start_main_window():
        main_window = MainWindow()
        main_window.show()
        splash.close()

    # Show splash for 2 seconds (adjust as needed)
    QTimer.singleShot(2000, start_main_window)

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
