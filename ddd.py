#!/usr/bin/env python3
"""
Robust PyQt6 Teams-like splash: inline SVG in QWebEngineView, pop + fade animations,
and reliable handoff to MainWindow.

Requirements:
    pip install PyQt6 PyQt6-WebEngine
Run this file from the folder containing your SVG (valid8r_microbounceslow.svg).
"""

import os
os.environ.setdefault(
    "QTWEBENGINE_CHROMIUM_FLAGS",
    "--enable-gpu-rasterization --enable-accelerated-2d-canvas --disable-gpu-vsync"
)

import sys
from pathlib import Path
from functools import partial

from PyQt6.QtCore import (
    Qt,
    QTimer,
    QRect,
    QUrl,
    QPropertyAnimation,
    QEasingCurve,
    QSequentialAnimationGroup,
    pyqtSignal,
)
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QFrame,
    QLabel,
    QMainWindow,
    QGraphicsDropShadowEffect,
    QGraphicsOpacityEffect,
)

from PyQt6.QtWebEngineWidgets import QWebEngineView


class TeamsLikeSplash(QWidget):
    finished = pyqtSignal()

    def __init__(self, svg_path: Path, size_px: int = 380, hold_ms: int = 2200):
        flags = Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint
        super().__init__(None, flags)

        self.svg_path = svg_path
        self.card_size = int(size_px)
        self.hold_ms = hold_ms

        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        try:
            # best-effort: don't steal focus
            self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, True)
        except Exception:
            pass

        # Card frame
        self.card = QFrame(self)
        self.card.setFixedSize(self.card_size, self.card_size)
        self.card.setStyleSheet("QFrame { background: rgba(255,255,255,0); border-radius: 18px; }")

        shadow = QGraphicsDropShadowEffect(self.card)
        shadow.setBlurRadius(36)
        shadow.setOffset(0, 12)
        shadow.setColor(QColor(0, 0, 0, 130))
        self.card.setGraphicsEffect(shadow)

        layout = QVBoxLayout(self.card)
        layout.setContentsMargins(0, 0, 0, 0)

        # Inline the SVG so animations run inside the DOM
        svg_text = svg_path.read_text(encoding="utf-8")
        html = f"""<!doctype html>
        <html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
        <style>html,body{{height:100%;margin:0;background:transparent;overflow:hidden}}
        .wrap{{display:flex;align-items:center;justify-content:center;height:100%;width:100%}}
        svg{{width:92%;height:92%;display:block}}</style></head>
        <body><div class="wrap">{svg_text}</div></body></html>"""

        self.view = QWebEngineView(self.card)
        try:
            # best-effort transparent page background
            self.view.page().setBackgroundColor(QColor(0, 0, 0, 0))
        except Exception:
            pass

        base = QUrl.fromLocalFile(str(svg_path.resolve().parent) + "/")
        self.view.setHtml(html, baseUrl=base)
        self.view.setFixedSize(self.card_size, self.card_size)
        layout.addWidget(self.view)

        # Keep references to animations (and parent them) to avoid GC issues.
        self._geom_group = QSequentialAnimationGroup(self)
        self._fade_in = QPropertyAnimation(self, b"windowOpacity", self)
        self._fade_out = QPropertyAnimation(self, b"windowOpacity", self)
        # note: we'll animate windowOpacity (works across platforms) rather than a separate
        # QGraphicsOpacityEffect. This reduces complexity and ensures animation has a parent.
        self.setWindowOpacity(0.0)

        # We'll still keep the card in place
        self.card.move(0, 0)

    def show_splash(self):
        screen = QApplication.primaryScreen()
        geom = screen.availableGeometry() if screen else QRect(0, 0, 1280, 800)

        total_w = self.card_size
        total_h = self.card_size
        final_x = geom.x() + (geom.width() - total_w) // 2
        final_y = geom.y() + (geom.height() - total_h) // 2 - 10

        start_w = max(24, int(total_w * 0.16))
        start_h = max(24, int(total_h * 0.16))
        start_x = final_x + (total_w - start_w) // 2
        start_y = final_y + (total_h - start_h) // 2

        overshoot_factor = 1.14
        overs_w = int(total_w * overshoot_factor)
        overs_h = int(total_h * overshoot_factor)
        overs_x = final_x - (overs_w - total_w) // 2
        overs_y = final_y - (overs_h - total_h) // 2

        # Ensure widget geometry is set (so animations know their target rects)
        self.setGeometry(final_x, final_y, total_w, total_h)
        self.card.move(0, 0)

        # --- geometry pop animations (parented to self) ---
        # Create anims with parent self to avoid GC
        anim1 = QPropertyAnimation(self, b"geometry", self)
        anim1.setDuration(420)
        anim1.setStartValue(QRect(start_x, start_y, start_w, start_h))
        anim1.setEndValue(QRect(overs_x, overs_y, overs_w, overs_h))
        anim1.setEasingCurve(QEasingCurve.Type.OutBack)

        anim2 = QPropertyAnimation(self, b"geometry", self)
        anim2.setDuration(220)
        anim2.setStartValue(QRect(overs_x, overs_y, overs_w, overs_h))
        anim2.setEndValue(QRect(final_x, final_y, total_w, total_h))
        anim2.setEasingCurve(QEasingCurve.Type.OutCubic)

        # Replace sequential group contents (ensure stable ownership)
        self._geom_group.clear()
        self._geom_group.addAnimation(anim1)
        self._geom_group.addAnimation(anim2)

        # --- fade-in (animate windowOpacity) ---
        self._fade_in.setDuration(320)
        self._fade_in.setStartValue(0.0)
        self._fade_in.setEndValue(1.0)
        self._fade_in.setEasingCurve(QEasingCurve.Type.InOutCubic)

        # fade-out configuration (prepared but started later)
        self._fade_out.setDuration(360)
        self._fade_out.setStartValue(1.0)
        self._fade_out.setEndValue(0.0)
        self._fade_out.setEasingCurve(QEasingCurve.Type.InOutCubic)
        # when fade_out completes => emit finished
        self._fade_out.finished.connect(self._on_faded)

        # geometry finished -> start hold timer
        self._geom_group.finished.connect(self._on_geom_finished)

        # Start: show widget, then start fade and geometry animations
        self.show()
        # ensure web engine docs begin loading
        QApplication.processEvents()
        self._fade_in.start()
        self._geom_group.start()

    def _on_geom_finished(self):
        # hold, then start fade out
        QTimer.singleShot(self.hold_ms, self._start_fade_out)

    def _start_fade_out(self):
        # Make sure fade_out is parented (we created it with parent)
        self._fade_out.start()

    def _on_faded(self):
        # emit finished BEFORE closing so listeners can react reliably
        try:
            self.finished.emit()
        except Exception:
            pass
        # close the splash window
        self.close()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Valid8r — Main")
        self.resize(960, 600)
        lbl = QLabel("Valid8r — Ready", self)
        lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        f = lbl.font()
        f.setPointSize(16)
        lbl.setFont(f)
        self.setCentralWidget(lbl)


def main():
    try:
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.ApplicationAttribute.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass

    app = QApplication(sys.argv)

    # Initially don't quit when last window closes while splash runs
    app.setQuitOnLastWindowClosed(False)

    svg_file = Path(__file__).parent / "valid8r_microbounceslow.svg"
    if not svg_file.exists():
        print("SVG not found:", svg_file.resolve())
        sys.exit(1)

    # Create windows once and keep references
    splash = TeamsLikeSplash(svg_file, size_px=380, hold_ms=3400)
    mainw = MainWindow()

    # connect splash finished to showing main window
    def show_main_and_enable_quit():
        # Post to the event loop to avoid reentrancy during the animation callback
        QTimer.singleShot(0, lambda: (
            mainw.show(),
            mainw.raise_(),
            mainw.activateWindow(),
            app.setQuitOnLastWindowClosed(True),
        ))

    splash.finished.connect(show_main_and_enable_quit)

    # Debug prints to stdout to see lifecycle in console
    print("Starting splash...")
    splash.show_splash()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()
