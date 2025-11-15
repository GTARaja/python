#!/usr/bin/env python3
"""
Teams-like splash, PyQt6 port.
Run this from the folder containing valid8r_microbounceslow.svg.

Install:
    pip install PyQt6 PyQt6-WebEngine
"""

import os
# Set Chromium flags BEFORE importing Qt modules
os.environ.setdefault(
    "QTWEBENGINE_CHROMIUM_FLAGS",
    "--enable-gpu-rasterization --enable-accelerated-2d-canvas --disable-gpu-vsync"
)

import sys
from pathlib import Path

from PyQt6.QtCore import (
    Qt,
    QTimer,
    QRect,
    QUrl,
    QPropertyAnimation,
    QEasingCurve,
    QSequentialAnimationGroup,
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

# import WebEngine after env flags
from PyQt6.QtWebEngineWidgets import QWebEngineView


class TeamsLikeSplash(QWidget):
    def __init__(self, svg_path: Path, size_px: int = 380, hold_ms: int = 2200):
        # Window flag constants use Qt.WindowType in PyQt6
        flags = Qt.WindowType.FramelessWindowHint | Qt.WindowType.WindowStaysOnTopHint
        super().__init__(None, flags)
        self.svg_path = svg_path
        self.card_size = int(size_px)
        self.hold_ms = hold_ms

        # Make window transparent and not steal focus
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        # Show without activating (may not be supported on all platforms)
        try:
            self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, True)
        except Exception:
            pass

        # Card frame that holds the web view
        self.card = QFrame(self)
        self.card.setFixedSize(self.card_size, self.card_size)
        # transparent background so the SVG's transparent areas show through
        self.card.setStyleSheet("QFrame { background: rgba(255,255,255,0); border-radius: 18px; }")

        shadow = QGraphicsDropShadowEffect(self.card)
        shadow.setBlurRadius(36)
        shadow.setOffset(0, 12)
        shadow.setColor(QColor(0, 0, 0, 130))
        self.card.setGraphicsEffect(shadow)

        layout = QVBoxLayout(self.card)
        layout.setContentsMargins(0, 0, 0, 0)

        # Inline SVG content (so animations run as inline SVG)
        svg_text = svg_path.read_text(encoding="utf-8")
        html = f"""<!doctype html>
        <html><head><meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1"/>
        <style>html,body{{height:100%;margin:0;background:transparent;overflow:hidden}}
        .wrap{{display:flex;align-items:center;justify-content:center;height:100%;width:100%}}
        svg{{width:92%;height:92%;display:block}}</style></head>
        <body><div class="wrap">{svg_text}</div></body></html>"""

        self.view = QWebEngineView(self.card)

        # Try to request a transparent background for the page (best-effort)
        try:
            # QWebEnginePage.setBackgroundColor exists on PyQt6 WebEnginePage
            self.view.page().setBackgroundColor(QColor(0, 0, 0, 0))
        except Except
