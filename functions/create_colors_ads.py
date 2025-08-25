import pandas as pd
import webcolors

def rgb_to_hex(r, g, b):
    try:
        if pd.isna(r) or pd.isna(g) or pd.isna(b):
            return None
        # Convert to float → int → clamp to 0–255
        r, g, b = [max(0, min(255, int(float(v)))) for v in (r, g, b)]
        return "#{:02x}{:02x}{:02x}".format(r, g, b)
    except Exception:
        return None

def add_color_columns(df):
    df["hex_color"] = df.apply(lambda row: rgb_to_hex(row["R"], row["G"], row["B"]), axis=1)
    # optional: map to closest CSS color

    def closest_color_name(hex_color):
        if not hex_color:
            return None
        try:
            return webcolors.hex_to_name(hex_color)
        except ValueError:
            return None
    df["color_name"] = df["hex_color"].apply(closest_color_name)
    return df