import math
import random
import struct
import zlib
from pathlib import Path


WIDTH = 1800
HEIGHT = 1100
OUT = Path("landing/assets/queuewatch-hero.png")


def clamp(value: int) -> int:
    return max(0, min(255, value))


def blend(dst: tuple[int, int, int], src: tuple[int, int, int], alpha: float) -> tuple[int, int, int]:
    return (
        clamp(int(dst[0] * (1 - alpha) + src[0] * alpha)),
        clamp(int(dst[1] * (1 - alpha) + src[1] * alpha)),
        clamp(int(dst[2] * (1 - alpha) + src[2] * alpha)),
    )


def set_px(img: list[list[tuple[int, int, int]]], x: int, y: int, color: tuple[int, int, int], alpha: float = 1.0) -> None:
    if 0 <= x < WIDTH and 0 <= y < HEIGHT:
        img[y][x] = blend(img[y][x], color, alpha)


def line(img: list[list[tuple[int, int, int]]], x0: int, y0: int, x1: int, y1: int, color: tuple[int, int, int], alpha: float = 1.0, width: int = 1) -> None:
    dx = abs(x1 - x0)
    dy = -abs(y1 - y0)
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    x, y = x0, y0
    radius = max(0, width // 2)
    while True:
        for ox in range(-radius, radius + 1):
            for oy in range(-radius, radius + 1):
                if ox * ox + oy * oy <= radius * radius + 1:
                    set_px(img, x + ox, y + oy, color, alpha)
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x += sx
        if e2 <= dx:
            err += dx
            y += sy


def rect(img: list[list[tuple[int, int, int]]], x: int, y: int, w: int, h: int, color: tuple[int, int, int], alpha: float = 1.0) -> None:
    for yy in range(max(0, y), min(HEIGHT, y + h)):
        for xx in range(max(0, x), min(WIDTH, x + w)):
            set_px(img, xx, yy, color, alpha)


def circle(img: list[list[tuple[int, int, int]]], cx: int, cy: int, radius: int, color: tuple[int, int, int], alpha: float = 1.0) -> None:
    r2 = radius * radius
    for y in range(cy - radius, cy + radius + 1):
        for x in range(cx - radius, cx + radius + 1):
            d2 = (x - cx) * (x - cx) + (y - cy) * (y - cy)
            if d2 <= r2:
                edge = max(0.25, 1 - (d2 / max(1, r2)) * 0.45)
                set_px(img, x, y, color, alpha * edge)


def write_png(path: Path, img: list[list[tuple[int, int, int]]]) -> None:
    raw = bytearray()
    for row in img:
        raw.append(0)
        for r, g, b in row:
            raw.extend((r, g, b))

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", WIDTH, HEIGHT, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(bytes(raw), 9))
        + chunk(b"IEND", b"")
    )


def main() -> None:
    random.seed(41)
    img = [[(17, 20, 18) for _ in range(WIDTH)] for _ in range(HEIGHT)]

    for y in range(HEIGHT):
        for x in range(WIDTH):
            east = x / WIDTH
            south = y / HEIGHT
            vignette = ((x - WIDTH * 0.63) ** 2 / (WIDTH * WIDTH) + (y - HEIGHT * 0.48) ** 2 / (HEIGHT * HEIGHT))
            noise = random.randint(-5, 5)
            base = (
                18 + int(26 * (1 - south)) + noise,
                23 + int(18 * east) + noise,
                21 + int(11 * (1 - vignette)) + noise,
            )
            img[y][x] = tuple(clamp(c) for c in base)

    # Soft control-room glass panels.
    rect(img, 820, 155, 735, 570, (8, 12, 12), 0.42)
    rect(img, 885, 225, 605, 82, (234, 241, 230), 0.10)
    rect(img, 885, 328, 605, 82, (234, 241, 230), 0.08)
    rect(img, 885, 431, 605, 82, (234, 241, 230), 0.07)
    rect(img, 885, 534, 605, 82, (234, 241, 230), 0.08)

    # Transmission network.
    nodes = [
        (955, 650, (71, 167, 133)),
        (1080, 540, (33, 132, 199)),
        (1240, 610, (230, 155, 68)),
        (1375, 475, (78, 161, 91)),
        (1490, 655, (196, 80, 62)),
        (1185, 385, (33, 132, 199)),
        (1000, 345, (78, 161, 91)),
        (1418, 310, (230, 155, 68)),
    ]
    edges = [(0, 1), (1, 2), (2, 4), (1, 5), (5, 7), (5, 6), (2, 3), (3, 7), (0, 2)]
    for a, b in edges:
        x0, y0, _ = nodes[a]
        x1, y1, _ = nodes[b]
        line(img, x0, y0, x1, y1, (154, 196, 184), 0.23, 7)
        line(img, x0, y0, x1, y1, (230, 238, 221), 0.34, 2)

    for x, y, color in nodes:
        for r, alpha in ((34, 0.06), (22, 0.11), (12, 0.8)):
            circle(img, x, y, r, color, alpha)
        circle(img, x, y, 5, (247, 250, 239), 0.95)

    # Data strips and queue deltas.
    colors = [(33, 132, 199), (71, 167, 133), (230, 155, 68), (196, 80, 62)]
    for i in range(18):
        x = 920 + (i % 6) * 94
        y = 255 + (i // 6) * 103
        rect(img, x, y, 54 + (i * 19) % 58, 7, colors[i % len(colors)], 0.82)
        rect(img, x, y + 18, 38 + (i * 13) % 94, 4, (232, 237, 224), 0.35)

    # Foreground terrain/solar rows.
    for i in range(16):
        y = 830 + i * 13
        start = 0 + i * 9
        line(img, start, y, 840 + i * 28, y - 78, (62, 78, 70), 0.65, 4)
        line(img, start + 14, y + 5, 850 + i * 28, y - 71, (28, 42, 42), 0.7, 2)

    # Utility queue change marker.
    rect(img, 1040, 760, 378, 98, (235, 241, 229), 0.12)
    rect(img, 1074, 794, 192, 11, (71, 167, 133), 0.90)
    rect(img, 1074, 820, 282, 7, (230, 238, 221), 0.30)

    write_png(OUT, img)


if __name__ == "__main__":
    main()
