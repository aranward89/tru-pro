
import pandas as pd
import numpy as np
import os
import unicodedata
import re

PHASE2_FILE = "phase2_team_rosters.csv"
OUTPUT_DIR = "phase3_class_outputs"
CURRENT_YEAR = 2024
os.makedirs(OUTPUT_DIR, exist_ok=True)

def run_phase3():
    print("[INFO] Loading Phase 2 team rosters...")
    df = pd.read_csv(PHASE2_FILE, low_memory=False, encoding="utf-8")
    df.columns = [col.strip().lower() for col in df.columns]

    df["player"] = df["player"].astype(str).str.strip()
    df["player"] = df["player"].apply(lambda x: unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("utf-8"))

    df["position"] = df.get("position", "F").fillna("F").astype(str).str.upper().str.strip()
    df["position"] = df["position"].replace({
        "F/D": "F", "D/F": "D", "FORWARD": "F", "DEFENSE": "D", "": "F", "-": "F",
        "LW": "F", "RW": "F", "C": "F", "LD": "D", "RD": "D"
    })
    df = df[df["position"].isin(["F", "D"])]

    df = df[~df["player"].str.strip().str.isnumeric()]
    df = df.drop_duplicates(subset=["player", "team", "gp", "g", "a", "birthyear"])
    df["gp"] = pd.to_numeric(df["gp"], errors="coerce")
    df["g"] = pd.to_numeric(df["g"], errors="coerce")
    df["a"] = pd.to_numeric(df["a"], errors="coerce")
    df = df.dropna(subset=["gp", "g", "a"])
    df = df[df["gp"] >= 10]
    df["birthyear"] = pd.to_numeric(df["birthyear"], errors="coerce")
    df = df[df["birthyear"] >= 1999]
    df["age"] = CURRENT_YEAR - df["birthyear"]

    for field in ["class 1", "class 2", "class 3"]:
        if field not in df.columns:
            continue
        for value in df[field].dropna().unique():
            group_df = df[df[field] == value].copy()
            if group_df.empty:
                continue

            group_df["actualppg"] = ((group_df["g"] + group_df["a"]) / group_df["gp"]).round(2)
            group_df["opponentrating"] = pd.to_numeric(group_df["opponentrating"], errors="coerce")
            opp_cutoff = group_df["opponentrating"].quantile(0.05)
            group_df = group_df[group_df["opponentrating"] >= opp_cutoff]

            mean_opp = group_df["opponentrating"].mean()
            range_opp = max(group_df["opponentrating"].max() - group_df["opponentrating"].min(), 1)
            sched_strength = (group_df["opponentrating"] - mean_opp) / range_opp
            sched_multiplier = (1 + (sched_strength * 2.5)).clip(0.5, 1.5)
            group_df["schedadjppg"] = (group_df["actualppg"] * sched_multiplier).round(2)

            mean_age = group_df["age"].mean()
            range_age = max(group_df["age"].max() - group_df["age"].min(), 1.5)
            age_factor = (mean_age - group_df["age"]) / range_age
            group_df["ageadjppg"] = (group_df["schedadjppg"] * (1 + age_factor * 1.5)).round(2)

            team_totals = group_df.groupby("team")[["g", "a"]].sum().sum(axis=1)
            player_points = group_df["g"] + group_df["a"]
            group_df["teampoints"] = group_df["team"].map(team_totals)
            group_df["pctteampoints"] = player_points / group_df["teampoints"].replace(0, np.nan)
            group_df["pctcentered"] = group_df["pctteampoints"] - group_df["pctteampoints"].mean()
            group_df["pctteampointsadjppg"] = (group_df["schedadjppg"] * (1 + group_df["pctcentered"] * 1.5)).round(2)

            group_df["truproscore"] = (
                (group_df["schedadjppg"] + group_df["ageadjppg"] + group_df["pctteampointsadjppg"]) / 3
            ).round(2)

            pos_frames = []
            for pos in ["F", "D"]:
                pos_df = group_df[group_df["position"] == pos].copy()
                if not pos_df.empty:
                    mean_pos = pos_df["truproscore"].mean()
                    std_pos = pos_df["truproscore"].std()
                    super_threshold = mean_pos + 2.5 * std_pos + std_pos
                    pos_df["positional_z_score"] = ((pos_df["truproscore"] - mean_pos) / std_pos).round(2)

                    def assign_prospect_grade(z, score):
                        if score >= super_threshold:
                            return "🌟🌟🌟🌟🌟 Star Prospect"
                        elif z > 2.5: return "⭐⭐⭐⭐ Elite Prospect"
                        elif z > 2.0: return "⭐⭐⭐ High-End Prospect"
                        elif z > 1.0: return "⭐⭐ Strong Prospect"
                        elif z > 0.5: return "⭐ Solid Prospect"
                        else: return ""

                    pos_df["prospect_grade"] = pos_df.apply(lambda row: assign_prospect_grade(row["positional_z_score"], row["truproscore"]), axis=1)
                    pos_df["prospectscore"] = pos_df["positional_z_score"]
                    pos_df["scheddifffromactual"] = (pos_df["schedadjppg"] - pos_df["actualppg"]).round(2)
                    pos_df["agedifffromactual"] = (pos_df["ageadjppg"] - pos_df["actualppg"]).round(2)
                    pos_df["truprodifffromactual"] = (pos_df["truproscore"] - pos_df["actualppg"]).round(2)
                    pos_frames.append(pos_df)

            class_df = pd.concat(pos_frames, axis=0, ignore_index=True)
            safe_name = re.sub(r'[^a-zA-Z0-9_]+', '_', str(value))[:60]
            class_df.to_csv(f"{OUTPUT_DIR}/{safe_name}.csv", index=False)
            print(f"[SAVED] {value} → {OUTPUT_DIR}/{safe_name}.csv")

if __name__ == "__main__":
    run_phase3()
