import pandas as pd
import webcolors
from configs.configs import COLOR, HEX_COLOR, R, G, B

try:
    from webcolors._definitions import _CSS3_NAMES_TO_HEX as CSS3_NAMES_TO_HEX
except ImportError:
    # fallback для старых версий
    CSS3_NAMES_TO_HEX = webcolors.CSS3_NAMES_TO_HEX

def rgb_to_hex(r, g, b):
    try:
        if pd.isna(r) or pd.isna(g) or pd.isna(b):
            return None
        # Convert to float → int → clamp to 0–255
        r, g, b = [max(0, min(255, int(float(v)))) for v in (r, g, b)]
        return "#{:02x}{:02x}{:02x}".format(r, g, b)
    except Exception:
        return None


def closest_color_name(hex_color):
    """Return exact CSS3 color name if available, else closest match."""
    if not hex_color:
        return None
    try:
        return webcolors.hex_to_name(hex_color, spec="css3")
    except ValueError:
        # no exact match → ищем ближайший
        rgb = webcolors.hex_to_rgb(hex_color)

        min_distance = float("inf")
        closest_name = None
        for name, value in CSS3_NAMES_TO_HEX.items():
            candidate_rgb = webcolors.hex_to_rgb(value)
            distance = sum((c1 - c2) ** 2 for c1, c2 in zip(rgb, candidate_rgb))
            if distance < min_distance:
                min_distance = distance
                closest_name = name
        return closest_name
    
def add_color_columns(df):
    # Convert RGB → hex
    df[HEX_COLOR] = df.apply(lambda row: rgb_to_hex(row[R], row[G], row[B]), axis=1)

    # Map hex → CSS3 color name (exact or closest)
    df[COLOR] = df[HEX_COLOR].apply(closest_color_name)

    return df