#!/usr/bin/env python3
"""
valid8r Lottie Splash (PyQt6 + Qt WebEngine)

Place valid8r_microbounce.json in the same directory as this script,
then run: python valid8r_splash_lottie.py

Requires:
    pip install PyQt6 PyQt6-WebEngine
"""

import sys
import os
from pathlib import Path

from PyQt6.QtCore import (
    Qt,
    QTimer,
    QPropertyAnimation,
    QRect,
    QEasingCurve,
    QUrl,
)
from PyQt6.QtGui import QColor
from PyQt6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QGraphicsOpacityEffect,
    QMainWindow,
    QLabel,
)
from PyQt6.QtWebEngineWidgets import QWebEngineView


class LottieSplash(QWidget):
    """
    Frameless splash window that loads a Lottie JSON using a QWebEngineView.
    """

    def __init__(
        self,
        lottie_json_path: str,
        width: int = 460,
        height: int = 460,
        hold_ms: int = 1500,
        parent=None,
    ):
        super().__init__(parent)

        self.lottie_json_path = os.path.abspath(lottie_json_path)
        if not os.path.exists(self.lottie_json_path):
            raise FileNotFoundError(f"Lottie file not found: {self.lottie_json_path}")

        # Window setup
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint
            | Qt.WindowType.WindowStaysOnTopHint
            | Qt.WindowType.Tool
        )
        # allows transparent background
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)

        self.resize(width, height)

        # opacity effect for smooth fade in/out
        self.opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.opacity_effect)
        self.opacity_effect.setOpacity(0.0)

        # web view for Lottie
        self.web_view = QWebEngineView(self)
        self.web_view.setContextMenuPolicy(Qt.ContextMenuPolicy.NoContextMenu)
        self.web_view.setZoomFactor(1.0)
        self.web_view.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.web_view.page().setBackgroundColor(QColor(0, 0, 0, 0))

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.web_view)

        # Load HTML that hosts the lottie-player and references the JSON by relative path.
        # Using baseUrl ensures the json loads via relative URL.
        base_dir = Path(self.lottie_json_path).parent.as_uri() + "/"
        html = self._html_for_lottie(Path(self.lottie_json_path).name, width, height)
        self.web_view.setHtml(html, QUrl(base_dir))

        # timing: how long we keep the splash visible (ms)
        self.hold_ms = hold_ms

        # animations placeholders
        self._fade_in_anim = None
        self._fade_out_anim = None
        self._geom_anim = None

        # when finished -> will call finished callback (set by caller)
        self.finished_callback = None

    def _html_for_lottie(self, json_filename: str, width: int, height: int) -> str:
        """
        Return a minimal HTML that includes lottie-player and loads the provided JSON filename.
        Uses unpkg CDN for lottie-player.
        """
        # Note: baseUrl must allow the JSON to be loaded by relative filename.
        # We set loop and autoplay.
        return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width,initial-scale=1" />
  <style>
    html,body{{height:100%;margin:0;background:transparent;overflow:hidden;}}
    .container{{display:flex;align-items:center;justify-content:center;height:100%;}}
    lottie-player{{width:{width}px;height:{height}px;display:block;}}
  </style>
</head>
<body>
  <div class="container">
    <lottie-player
      id="lottie"
      src="{json_filename}"
      background="transparent"
      speed="1"
      loop
      autoplay>
    </lottie-player>
  </div>

  <script src="https://unpkg.com/@lottiefiles/lottie-player@latest/dist/lottie-player.js"></script>
  <script>
    // Optional: forward events to Qt via console.log so we can detect readiness if needed.
    const pl = document.getElementById('lottie');
    pl.addEventListener('ready', () => console.log('LOTTIE_READY'));
    pl.addEventListener('complete', () => console.log('LOTTIE_COMPLETE'));
  </script>
</body>
</html>
"""

    def show_splash(self, center=True, pop_overshoot=1.12, pop_ms=450):
        """Show splash with pop (geometry overshoot) and fade in. After hold_ms, fade out and call finished."""
        # position on screen
        screen = QApplication.primaryScreen()
        geom = screen.availableGeometry() if screen else QRect(0, 0, 800, 600)
        w = self.width()
        h = self.height()
        final_x = geom.x() + (geom.width() - w) // 2
        final_y = geom.y() + (geom.height() - h) // 2 - 20

        # start small near center for pop effect
        start_w = max(24, int(w * 0.16))
        start_h = max(24, int(h * 0.16))
        start_x = final_x + (w - start_w) // 2
        start_y = final_y + (h - start_h) // 2

        overshoot_w = int(w * pop_overshoot)
        overshoot_h = int(h * pop_overshoot)
        overshoot_x = final_x - (overshoot_w - w) // 2
        overshoot_y = final_y - (overshoot_h - h) // 2

        # set geometry to final now (we'll animate from start to overshoot->final)
        self.setGeometry(final_x, final_y, w, h)
        self.show()

        # Geometry animation (start -> overshoot -> final)
        from PyQt6.QtCore import QSequentialAnimationGroup

        self._geom_anim = QSequentialAnimationGroup(self)
        anim1 = QPropertyAnimation(self, b"geometry", self)
        anim1.setDuration(pop_ms)
        anim1.setStartValue(QRect(start_x, start_y, start_w, start_h))
        anim1.setEndValue(QRect(overshoot_x, overshoot_y, overshoot_w, overshoot_h))
        anim1.setEasingCurve(QEasingCurve.Type.OutBack)

        anim2 = QPropertyAnimation(self, b"geometry", self)
        anim2.setDuration(int(pop_ms * 0.55))
        anim2.setStartValue(QRect(overshoot_x, overshoot_y, overshoot_w, overshoot_h))
        anim2.setEndValue(QRect(final_x, final_y, w, h))
        anim2.setEasingCurve(QEasingCurve.Type.OutCubic)

        self._geom_anim.addAnimation(anim1)
        self._geom_anim.addAnimation(anim2)

        # Fade-in
        self._fade_in_anim = QPropertyAnimation(self.opacity_effect, b"opacity", self)
        self._fade_in_anim.setDuration(360)
        self._fade_in_anim.setStartValue(0.0)
        self._fade_in_anim.setEndValue(1.0)
        self._fade_in_anim.setEasingCurve(QEasingCurve.Type.InOutCubic)

        # when geometry finished, start hold timer
        self._geom_anim.finished.connect(self._start_hold_timer)

        # start animations
        self._fade_in_anim.start()
        self._geom_anim.start()

    def _start_hold_timer(self):
        QTimer.singleShot(self.hold_ms, self._start_fade_out)

    def _start_fade_out(self):
        self._fade_out_anim = QPropertyAnimation(self.opacity_effect, b"opacity", self)
        self._fade_out_anim.setDuration(420)
        self._fade_out_anim.setStartValue(1.0)
        self._fade_out_anim.setEndValue(0.0)
        self._fade_out_anim.setEasingCurve(QEasingCurve.Type.InOutCubic)
        self._fade_out_anim.finished.connect(self._on_faded)
        self._fade_out_anim.start()

    def _on_faded(self):
        self.hide()
        if callable(self.finished_callback):
            self.finished_callback()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("valid8r — Main Window")
        self.resize(960, 600)

        label = QLabel("valid8r — ready", self)
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        font = label.font()
        font.setPointSize(18)
        label.setFont(font)
        self.setCentralWidget(label)


def main():
    app = QApplication(sys.argv)

    # Ensure web engine is available - import already done above
    script_dir = Path(__file__).parent if "__file__" in globals() else Path.cwd()
    lottie_path = script_dir / "valid8r_microbounce.json"
    if not lottie_path.exists():
        # fallback: try absolute path where file was saved previously
        fallback = Path("/mnt/data/valid8r_microbounce.json")
        if fallback.exists():
            lottie_path = fallback
        else:
            raise FileNotFoundError("valid8r_microbounce.json not found in script folder or /mnt/data/")

    splash = LottieSplash(str(lottie_path), width=560, height=560, hold_ms=1400)

    main_win = MainWindow()

    # when splash finished, show main window
    splash.finished_callback = lambda: (main_win.show(), main_win.raise_(), main_win.activateWindow())

    splash.show_splash()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
