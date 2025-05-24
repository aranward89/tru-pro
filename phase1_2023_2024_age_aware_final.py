import pandas as pd
import re
import unicodedata
import difflib
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# === CONFIG ===
PHASE1_INPUT = "league6_2023-2024.csv"
REFERENCE_MAPPING = "phase1_team_mapping.csv"
SCRAPED_OUTPUT = "scraped_new_teams.csv"
FINAL_OUTPUT = "phase1output.csv"

# === AGE/CLASS MAPPING ===

def get_birth_year_from_code(age_code, season_end_year, is_canadian):
    try:
        code_int = int(age_code)
        birth_year = 2000 + code_int
        return birth_year
    except ValueError:
        return None

def get_class_from_birth_year(birth_year, season_end_year, is_canadian):
    if birth_year is None:
        return None
    diff = season_end_year - birth_year
    if is_canadian:
        diff -= 1
    return f"U{diff}"


# === HELPERS ===
def normalize_name(name):
    if not isinstance(name, str):
        return ""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("utf-8")
    name = re.sub(r"[^a-z0-9]+", " ", name.lower()).strip()
    return name

def extract_age_level(text):
    match = re.search(r"(?:u\s?(\d{2})|(\d{2})\s?u)", text.lower())
    if match:
        return match.group(1) or match.group(2)
    return ""

def get_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def extract_links_from_cell(cell):
    links = cell.find_all("a")
    ep_url = None
    other_links = []
    for link in links:
        href = link.get("href", "")
        if "eliteprospects.com" in href:
            ep_url = href
        else:
            other_links.append(href)
    return ep_url, other_links

def scrape_mhr_with_links(driver, url):
    print(f"\U0001f4f0 Scraping MHR: {url}")
    driver.get(url)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table")
    if not table:
        print("⚠️ No MHR table found")
        return pd.DataFrame()

    rows = table.find_all("tr")[1:]
    results = []
    seen_teams = set()

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 7:
            continue

        team = cols[1].text.strip()
        if team in seen_teams:
            continue
        seen_teams.add(team)

        record = cols[2].text.strip()
        rating = re.sub(r"[^\d.]+", "", cols[3].text.strip())
        agd = re.sub(r"[^\d.-]+", "", cols[4].text.strip())
        sched = re.sub(r"[^\d.]+", "", cols[5].text.strip())
        ep_url, other_links = extract_links_from_cell(cols[6])

        results.append({
            "Team": team,
            "NormalizedTeam": normalize_name(team),
            "AgeLevel": extract_age_level(team),
            "Record": record,
            "TeamRating": rating,
            "AGD": agd,
            "OpponentRating": sched,
            "EP_URL": ep_url,
            "OtherLinks": ", ".join(other_links)
        })

        if len(results) >= 200:
            print("🔴 Reached 200 teams, stopping scrape for this page.")
            break

    return pd.DataFrame(results)


def extract_numeric_class(class_str):
    match = re.search(r"U(\d{2})", str(class_str).upper())
    return int(match.group(1)) if match else None

def extract_ep_class_from_url(url):
    match = re.search(r"-u(\d{2})", str(url).lower())
    return int(match.group(1)) if match else None

def match_to_ep(scraped_df, ref_df):
    matched_rows = []
    used_refs = set()

    for _, row in scraped_df.iterrows():
        norm_team = row["NormalizedTeam"]
        team_class_num = extract_numeric_class(row.get("Class 1", ""))

        candidates = []
        for _, ref_row in ref_df.iterrows():
            ref_norm = ref_row["NormalizedTeam"]
            ep_url = ref_row.get("EP_URL", "")
            ref_class_num = extract_ep_class_from_url(ep_url)

            if ref_row["NormalizedTeam"] in used_refs:
                continue
            if team_class_num and ref_class_num and abs(team_class_num - ref_class_num) > 1:
                continue

            score = difflib.SequenceMatcher(None, norm_team, ref_norm).ratio()
            candidates.append((score, ref_row))

        if not candidates:
            continue

        # Pick best match by score
        candidates.sort(reverse=True, key=lambda x: x[0])
        best_score, best_match = candidates[0]

        matched_rows.append({
            "Team": row["Team"],
            "EP_URL": best_match["EP_URL"],
            "Level": best_match["Level"],
            "Class 1": best_match.get("Class 1", ""),
            "Class 2": best_match.get("Class 2", ""),
            "Class 3": best_match.get("Class 3", ""),
            "Season": row.get("Season", "2023-2024"),
            "TeamRating": row["TeamRating"],
            "OpponentRating": row["OpponentRating"]
        })
        used_refs.add(best_match["NormalizedTeam"])

    return pd.DataFrame(matched_rows)


def run_scraper():
    leagues = pd.read_csv(PHASE1_INPUT)
    ref = pd.read_csv(REFERENCE_MAPPING)
    ref["NormalizedTeam"] = ref["Team"].apply(normalize_name)

    driver = get_driver()
    all_data = []

    for _, row in leagues.iterrows():
        mhr_url = row.get("MHR", "")
        if not isinstance(mhr_url, str) or not mhr_url.startswith("http"):
            print(f"⚠️ Skipping invalid MHR URL: {mhr_url}")
            continue

        df = scrape_mhr_with_links(driver, mhr_url)
        if df.empty:
            continue

        context = {
            "Level": str(row.get("Level", "")),
            "Class 1": str(row.get("Class 1", "")),
            "Class 2": str(row.get("Class 2", "")),
            "Class 3": str(row.get("Class 3", "")),
            "Season": str(row.get("Season", "2023-2024"))
        }
        for key, value in context.items():
            df[key] = value

        # === AGE MAPPING ===
        season_end = int(context["Season"].split("-")[1])
        df["IsCanadian"] = df.apply(lambda x: any("can" in str(context[k]).lower() for k in ["Class 1", "Class 2", "Class 3"]), axis=1)
        df["BirthYear"] = df["AgeLevel"].apply(lambda code: get_birth_year_from_code(code, season_end, False))
        df["BirthYear"] = df.apply(lambda row: get_birth_year_from_code(row["AgeLevel"], season_end, row["IsCanadian"]), axis=1)
        df["ClassLevel"] = df.apply(lambda row: get_class_from_birth_year(row["BirthYear"], season_end, row["IsCanadian"]), axis=1)

        all_data.append(df)

    driver.quit()

    if not all_data:
        print("❌ No data scraped. Aborting.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    full_df.to_csv(SCRAPED_OUTPUT, index=False)
    print(f"✅ Scraped data saved to: {SCRAPED_OUTPUT}")

    matched_df = match_to_ep(full_df, ref)
    matched_df.to_csv(FINAL_OUTPUT, index=False)
    print(f"✅ Final Phase 1 mapping written to: {FINAL_OUTPUT}")

if __name__ == "__main__":
    run_scraper()
