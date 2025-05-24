import pandas as pd
import numpy as np
import os
import re
import time
import unicodedata
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup

from closerun import get_driver, login_ep

PHASE1_FILE = "phase1output.csv"
OUTPUT_FILE = "phase2_team_rosters.csv"
MISSING_ROSTER_FILE = "missing_rosters_for_manual_input.csv"


def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('utf-8')
    name = re.sub(r'[^a-zA-Z0-9 ]', '', name)
    return name.strip().lower()


def normalize_local_stats(df):
    df = df.rename(columns=lambda x: x.strip())
    df = df.rename(columns={
        'GamesPlayed': 'GP', 'Goals': 'G', 'Assists': 'A',
    })
    for col in ['Player', 'GP', 'G', 'A']:
        if col not in df.columns:
            df[col] = np.nan
    df["PPG"] = df.apply(lambda r: (r["G"] + r["A"]) / r["GP"] if r["GP"] > 0 else 0, axis=1)
    if "Position" not in df.columns:
        df["Position"] = "F"
    return df


def scrape_ep_team_roster(driver, base_url, team_name, retries=3, delay=5):
    for attempt in range(retries):
        try:
            print(f"🌀 Attempt {attempt+1} for {team_name}")
            driver.get(base_url)
            time.sleep(3)
            soup = BeautifulSoup(driver.page_source, "html.parser")
            # Grab logo URL from EP team page
            logo_url = ""
            logo_el = soup.select_one("img[src*='team-logos']") or soup.select_one("img.TeamHeader_logo__")
            if logo_el and logo_el.has_attr("src"):
                logo_url = logo_el["src"]

            table = soup.find("table", class_=lambda c: c and c.startswith("SortTable_table"))
            if not table:
                raise ValueError("No roster table found")

            roster = []
            current_section = ""
            for row in table.find_all("tr"):
                if row.find("th"):
                    section = row.get_text(strip=True).upper()
                    if "GOALTENDER" in section:
                        current_section = "G"
                    elif "DEFENSE" in section:
                        current_section = "D"
                    elif "FORWARD" in section:
                        current_section = "F"
                    continue

                cols = row.find_all("td")
                if len(cols) < 1:
                    continue

                jersey = nationality = player_name = position = birth_year = ""

                for col in cols:
                    text = col.get_text(strip=True)
                    if not jersey and re.match(r"#?\d{1,2}$", text):
                        jersey = text
                        continue
                    if not nationality:
                        img = col.select_one("div.DualFlag_flagWrapper__Qkagc img") or col.select_one("img[alt]")
                        if img and "flag" in img.get("alt", "").lower():
                            nationality = img["alt"].split()[0]
                        continue
                    if not birth_year and re.match(r"19\d{2}|20[0-2]\d", text):
                        birth_year = text
                        continue
                    if not player_name:
                        match = re.search(r"(.*?)\s*\(([A-Z]+)\)", text)
                        if match:
                            player_name = match.group(1).strip()
                            position = match.group(2).strip()
                        else:
                            player_name = text.strip()
                        continue

                position = position or current_section or "F"
                if player_name:
                    roster.append({
                        "Player": player_name,
                        "Team": team_name,
                        "Position": position,
                        "BirthYear": birth_year,
                        "Nationality": nationality,
                        "Jersey": jersey
                    })
            return pd.DataFrame(roster)

        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed for {team_name}: {e}")
            time.sleep(delay)
    print(f"❌ Final fail: {team_name}")
    return pd.DataFrame()


def scrape_ep_team_stats(driver, base_url, retries=3, delay=5):
    stats_url = base_url + "?tab=stats"
    for attempt in range(retries):
        try:
            print(f"📊 Attempt {attempt+1} to scrape stats: {stats_url}")
            driver.get(stats_url)
            time.sleep(3)
            stats = []
            WebDriverWait(driver, 10).until(
                lambda d: len(d.find_elements(By.CSS_SELECTOR, "table.SortTable_table__jnnJk tbody tr")) > 0
            )
            rows = driver.find_elements(By.CSS_SELECTOR, "table.SortTable_table__jnnJk tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) < 6:
                    continue
                try:
                    name_pos = cols[2].text.strip()
                    match = re.match(r"(.*?)\s*\((\w+)\)", name_pos)
                    player = match.group(1).strip() if match else name_pos
                    position = match.group(2) if match else "F"
                    gp, g, a = cols[3].text.strip(), cols[4].text.strip(), cols[5].text.strip()
                    if not (gp.isdigit() and g.isdigit() and a.isdigit()):
                        continue
                    gp, g, a = int(gp), int(g), int(a)
                    ppg = round((g + a) / gp, 4) if gp > 0 else 0
                    stats.append({"Player": player, "Position": position, "GP": gp, "G": g, "A": a, "PPG": ppg})
                except:
                    continue
            return pd.DataFrame(stats)
        except Exception as e:
            print(f"⚠️ Attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    print(f"❌ Final stats scrape fail for: {base_url}")
    return pd.DataFrame()


def run_phase2():
    df = pd.read_csv(PHASE1_FILE)

    MAX_TEAMS_PER_DRIVER = 30
    scrape_counter = 0
    driver = get_driver()
    login_ep(driver)

    all_data = []
    missing_rosters = []

    for idx, (_, row) in enumerate(df.iterrows()):
        if scrape_counter % MAX_TEAMS_PER_DRIVER == 0 and scrape_counter > 0:
            print(f"🔁 Restarting Chrome after {MAX_TEAMS_PER_DRIVER} teams...")
            driver.quit()
            time.sleep(2)
            driver = get_driver()
            login_ep(driver)

        scrape_counter += 1

        team = row['Team']
        ep_url = row.get('EP_URL')
        local_file = row.get("LocalStatsFile")

        if pd.isna(ep_url):
            print(f"❌ Skipping {team}, no EP URL")
            continue

        print(f"📥 Scraping: {team} | {ep_url}")
        try:
            stats_df = pd.DataFrame()
            if pd.notna(local_file) and os.path.exists(local_file):
                print(f"📂 Loading local stats from: {local_file}")
                stats_df = normalize_local_stats(pd.read_csv(local_file))
            else:
                print(f"❌ LocalStatsFile not found or empty. Falling back to scrape.")
                stats_df = scrape_ep_team_stats(driver, ep_url)

                # Also get roster metadata (birth year, nationality, etc.)
                roster_df = scrape_ep_team_roster(driver, ep_url, team)
                if not roster_df.empty:
                    roster_df["Player_norm"] = roster_df["Player"].apply(normalize_name)
                    stats_df["Player_norm"] = stats_df["Player"].apply(normalize_name)
                    stats_df = stats_df.merge(
                        roster_df[["Player_norm", "BirthYear", "Nationality", "Jersey"]],
                        on="Player_norm", how="left"
                    )

            if stats_df.empty:
                print(f"⚠️ No players found for {team}, consider local ingest.")
                missing_rosters.append({"Team": team, "EP_URL": ep_url, "Note": "No stats found"})
                continue

            stats_df["Player"] = stats_df["Player"].apply(lambda x: unicodedata.normalize('NFKD', str(x)).encode('ascii', 'ignore').decode('utf-8'))
            stats_df["Team"] = team

        # Extract EP_Team_ID from EP_URL and build TeamLogoFile
        ep_url = str(row.get("EP_URL", ""))
        match = re.search(r"/team/(\d+)", ep_url)
        ep_team_id = match.group(1) if match else ""
        stats_df["EP_Team_ID"] = ep_team_id
        stats_df["TeamLogoFile"] = ep_team_id + ".jpg" if ep_team_id else ""


            for col in ['EP_URL', 'Level', 'Class 1', 'Class 2', 'Class 3', 'Season', 'OpponentRating']:
                stats_df[col] = row.get(col, None)

            all_data.append(stats_df)
            print(f"✅ Appended {len(stats_df)} players from {team}")

        except Exception as e:
            print(f"❌ Error scraping {team}: {e}")
            continue

    driver.quit()

    if not all_data:
        print("❌ No player data collected after Phase2 scrape. Aborting save.")
        return

    final_df = pd.concat(all_data, ignore_index=True)
    final_df["GP"] = pd.to_numeric(final_df["GP"], errors="coerce").fillna(0)

    team_flag = (
        final_df.groupby("Team")["GP"]
        .apply(lambda g: (g < 10).all())
        .rename("StatsMissing")
    )
    final_df = final_df.merge(team_flag, on="Team", how="left")

    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Phase 2 complete. Saved final output with {len(final_df)} players to {OUTPUT_FILE}")

    if missing_rosters:
        pd.DataFrame(missing_rosters).to_csv(MISSING_ROSTER_FILE, index=False)
        print(f"⚠️ Saved missing roster log to {MISSING_ROSTER_FILE}")


if __name__ == "__main__":
    run_phase2()
