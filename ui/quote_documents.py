"""
Resize/compress uploaded images before base64-encoding for DIC quote payloads.
"""
from __future__ import annotations

import io
import os
from typing import Tuple

# Longest edge (px); env overrides for ops tuning.
_MAX_SIDE = int(os.environ.get("QUOTE_DOC_IMAGE_MAX_SIDE", "2048"))
_JPEG_Q = int(os.environ.get("QUOTE_DOC_JPEG_QUALITY", "82"))


def optimize_quote_document_bytes(raw: bytes, original_name: str) -> Tuple[bytes, str]:
    """
    Downscale and re-encode raster images (JPEG/PNG/WebP/BMP/TIFF) to compressed JPEG.
    PDFs and non-image uploads are returned unchanged.

    Returns (bytes, effective_filename_hint) — second value is only used for internal logic;
    callers may ignore it when the document label is human-readable.
    """
    original_name = original_name or "document"
    ext = original_name.lower().rsplit(".", 1)[-1] if "." in original_name else ""
    if ext not in ("jpg", "jpeg", "png", "webp", "bmp", "tif", "tiff"):
        return raw, original_name
    try:
        from PIL import Image, ImageOps
    except ImportError:
        return raw, original_name
    try:
        img = Image.open(io.BytesIO(raw))
        if getattr(img, "n_frames", 1) > 1:
            img.seek(0)
        img = ImageOps.exif_transpose(img)
        w, h = img.size
        if max(w, h) > _MAX_SIDE:
            try:
                resample = Image.Resampling.LANCZOS
            except AttributeError:
                resample = Image.LANCZOS
            img.thumbnail((_MAX_SIDE, _MAX_SIDE), resample)
        if img.mode in ("RGBA", "LA", "P"):
            base = Image.new("RGB", img.size, (255, 255, 255))
            if img.mode == "P":
                img = img.convert("RGBA")
            if img.mode in ("RGBA", "LA"):
                base.paste(img, mask=img.split()[-1])
            else:
                base.paste(img.convert("RGBA"))
            img = base
        elif img.mode != "RGB":
            img = img.convert("RGB")
        buf = io.BytesIO()
        img.save(buf, format="JPEG", quality=_JPEG_Q, optimize=True)
        out = buf.getvalue()
        stem = original_name.rsplit(".", 1)[0] if "." in original_name else original_name
        new_name = f"{stem}.jpg"
        if len(out) < len(raw):
            return out, new_name
        return raw, original_name
    except Exception:
        return raw, original_name
