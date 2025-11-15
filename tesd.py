#!/usr/bin/env python3
"""
Teams-like splash, with Chromium acceleration flags + inline SVG.
Run this from the folder containing valid8r_microbounce.svg.

Install:
    pip install PyQt5 PyQtWebEngine
"""

import os
# Try to help WebEngine performance — set flags before importing PyQt modules.
# Note: these flags may vary platform to platform; they often help with rendering.
os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS",
                      "--enable-gpu-rasterization --enable-accelerated-2d-canvas --disable-gpu-vsync")

import sys
from pathlib import Path
from PyQt5.QtCore import (
    Qt, QTimer, QRect, QUrl,
    QPropertyAnimation, QEasingCurve, QSequentialAnimationGroup
)
from PyQt5.QtGui import QColor
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QFrame, QLabel, QMainWindow,
    QGraphicsDropShadowEffect, QGraphicsOpacityEffect
)
# import WebEngine after setting env flags
from PyQt5.QtWebEngineWidgets import QWebEngineView

class TeamsLikeSplash(QWidget):
    def __init__(self, svg_path: Path, size_px: int = 380, hold_ms: int = 2200):
        super().__init__(None, Qt.Window | Qt.FramelessWindowHint | Qt.WindowStaysOnTopHint)
        self.svg_path = svg_path
        self.card_size = int(size_px)
        self.hold_ms = hold_ms

        self.setAttribute(Qt.WA_TranslucentBackground)
        self.setAttribute(Qt.WA_ShowWithoutActivating)

        # create card
        self.card = QFrame(self)
        self.card.setFixedSize(self.card_size, self.card_size)
        self.card.setStyleSheet("QFrame { background: rgba(255,255,255,0); border-radius: 18px; }")
        shadow = QGraphicsDropShadowEffect(self.card)
        shadow.setBlurRadius(36); shadow.setOffset(0, 12); shadow.setColor(QColor(0,0,0,130))
        self.card.setGraphicsEffect(shadow)

        layout = QVBoxLayout(self.card)
        layout.setContentsMargins(0, 0, 0, 0)

        # inline svg content (so animations run as inline SVG)
        svg_text = svg_path.read_text(encoding="utf-8")
        html = f"""<!doctype html>
        <html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
        <style>html,body{{height:100%;margin:0;background:transparent;overflow:hidden}}
        .wrap{{display:flex;align-items:center;justify-content:center;height:100%;width:100%}}
        svg{{width:92%;height:92%;display:block}}</style></head>
        <body><div class="wrap">{svg_text}</div></body></html>"""

        self.view = QWebEngineView(self.card)
        # transparent
        try:
            self.view.page().setBackgroundColor(QColor(0,0,0,0))
        except Exception:
            pass
        base = QUrl.fromLocalFile(str(svg_path.resolve().parent) + "/")
        self.view.setHtml(html, baseUrl=base)
        self.view.setFixedSize(self.card_size, self.card_size)
        layout.addWidget(self.view)

        self.opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.opacity_effect)
        self.opacity_effect.setOpacity(0.0)

        # animation placeholders
        self._geom_group = None
        self._fade_in = None
        self._fade_out = None

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

        self.setGeometry(final_x, final_y, total_w, total_h)
        self.card.move(0, 0)

        self._geom_group = QSequentialAnimationGroup()
        anim1 = QPropertyAnimation(self, b"geometry")
        anim1.setDuration(420)
        anim1.setStartValue(QRect(start_x, start_y, start_w, start_h))
        anim1.setEndValue(QRect(overs_x, overs_y, overs_w, overs_h))
        anim1.setEasingCurve(QEasingCurve.OutBack)

        anim2 = QPropertyAnimation(self, b"geometry")
        anim2.setDuration(220)
        anim2.setStartValue(QRect(overs_x, overs_y, overs_w, overs_h))
        anim2.setEndValue(QRect(final_x, final_y, total_w, total_h))
        anim2.setEasingCurve(QEasingCurve.OutCubic)

        self._geom_group.addAnimation(anim1)
        self._geom_group.addAnimation(anim2)

        self._fade_in = QPropertyAnimation(self.opacity_effect, b"opacity")
        self._fade_in.setDuration(320)
        self._fade_in.setStartValue(0.0)
        self._fade_in.setEndValue(1.0)
        self._fade_in.setEasingCurve(QEasingCurve.InOutCubic)

        self._geom_group.finished.connect(self._on_geom_finished)
        self._fade_in.start()
        self._geom_group.start()
        self.show()

    def _on_geom_finished(self):
        QTimer.singleShot(self.hold_ms, self._start_fade_out)

    def _start_fade_out(self):
        self._fade_out = QPropertyAnimation(self.opacity_effect, b"opacity")
        self._fade_out.setDuration(360)
        self._fade_out.setStartValue(1.0)
        self._fade_out.setEndValue(0.0)
        self._fade_out.setEasingCurve(QEasingCurve.InOutCubic)
        self._fade_out.finished.connect(self._on_faded)
        self._fade_out.start()

    def _on_faded(self):
        self.close()

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Valid8r — Main")
        self.resize(960,600)
        lbl = QLabel("Valid8r — Ready", self)
        lbl.setAlignment(Qt.AlignCenter)
        f=lbl.font(); f.setPointSize(16); lbl.setFont(f)
        self.setCentralWidget(lbl)

def main():
    # High DPI hints
    try:
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    except Exception:
        pass

    app = QApplication(sys.argv)
    svg_file = Path(__file__).parent / "valid8r_microbounceslow.svg"
    if not svg_file.exists():
        print("SVG not found:", svg_file.resolve())
        sys.exit(1)

    splash = TeamsLikeSplash(svg_file, size_px=380, hold_ms=3400)
    mainw = MainWindow()
    # total duration approx: pop(420+220)+fade(320)+hold+fadeout(360)
    total = 420+220+320+splash.hold_ms+360
    splash.show_splash()
    QTimer.singleShot(total, lambda: (mainw.show(), mainw.raise_(), mainw.activateWindow()))
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
